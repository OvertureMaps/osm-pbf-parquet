use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures_util::pin_mut;
use log::info;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufReader;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use osmpbf::{AsyncBlobReader, BlobDecode, Element, PrimitiveBlock};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::error::{OsmPbfParquetError, OsmPbfParquetResult};
use crate::osm_arrow::OSMType;
use crate::sink::ElementSink;
use crate::util::{ARGS, ELEMENT_COUNTER, SinkpoolStore};

pub async fn create_s3_buf_reader(url: Url) -> OsmPbfParquetResult<BufReader> {
    let s3_store = AmazonS3Builder::from_env().with_url(url.clone()).build()?;
    let path = Path::parse(url.path())?;
    let meta = s3_store.head(&path).await?;
    Ok(BufReader::with_capacity(
        Arc::new(s3_store),
        &meta,
        ARGS.get()
            .expect("ARGS not initialized")
            .get_input_buffer_size_bytes(),
    ))
}

pub async fn create_local_buf_reader(path: &str) -> OsmPbfParquetResult<BufReader> {
    let local_store: LocalFileSystem = LocalFileSystem::new();
    let path = std::path::Path::new(path);
    let filesystem_path = object_store::path::Path::from_filesystem_path(path)?;
    let meta = local_store.head(&filesystem_path).await?;
    Ok(BufReader::with_capacity(
        Arc::new(local_store),
        &meta,
        ARGS.get()
            .expect("ARGS not initialized")
            .get_input_buffer_size_bytes(),
    ))
}

pub async fn process_blobs(
    buf_reader: BufReader,
    sinkpools: Arc<SinkpoolStore>,
) -> OsmPbfParquetResult<()> {
    let mut blob_reader = AsyncBlobReader::new(buf_reader);

    let stream = blob_reader.stream();
    pin_mut!(stream);

    let filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>> = Arc::new(HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(0))),
        (OSMType::Way, Arc::new(Mutex::new(0))),
        (OSMType::Relation, Arc::new(Mutex::new(0))),
    ]));

    // Avoid too many tasks in memory
    let active_tasks = (1.5
        * ARGS
            .get()
            .expect("ARGS not initialized")
            .get_worker_threads() as f32) as usize;
    let semaphore = Arc::new(Semaphore::new(active_tasks));

    let mut join_set = JoinSet::new();
    while let Some(Ok(blob)) = stream.next().await {
        let sinkpools = sinkpools.clone();
        let filenums = filenums.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        join_set.spawn(async move {
            let result = match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) => Ok(()),
                Ok(BlobDecode::OsmData(block)) => {
                    process_block(block, sinkpools, filenums).await.map(|_| ())
                }
                Ok(BlobDecode::Unknown(unknown)) => {
                    Err(OsmPbfParquetError::UnknownBlobType(unknown.to_string()))
                }
                Err(error) => Err(OsmPbfParquetError::BlobDecode(error.to_string())),
            };
            drop(permit);
            result
        });
    }
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    Ok(())
}

pub async fn monitor(sinkpools: Arc<SinkpoolStore>, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.tick().await; // First tick is immediate

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                // Run cleanup
                if let Err(e) = finish_sinks(sinkpools.clone(), false).await {
                    info!("Monitor cleanup error: {}", e);
                }

                // Log progress
                let processed = ELEMENT_COUNTER.load(std::sync::atomic::Ordering::Relaxed);
                let processed_str = if processed >= 1_000_000_000 {
                    format!("{:.2}B", (processed as f64) / 1_000_000_000.0)
                } else if processed >= 1_000_000 {
                    format!("{:.2}M", (processed as f64) / 1_000_000.0)
                } else {
                    format!("{}", processed)
                };
                info!("Processed {} elements", processed_str);
            }
        }
    }
}

pub async fn finish_sinks(
    sinkpools: Arc<SinkpoolStore>,
    force_finish: bool,
) -> OsmPbfParquetResult<()> {
    let handle = Handle::current();
    let mut join_set = JoinSet::new();
    for sinkpool in sinkpools.values() {
        let mut pool = sinkpool.lock().expect("sinkpool mutex lock failed");
        let sinks = pool.drain(..).collect::<Vec<_>>();
        for mut sink in sinks {
            if force_finish || sink.last_write_cycle.elapsed().as_secs() > 30 {
                // Finish, old or final cleanup run
                join_set.spawn_on(
                    async move {
                        sink.finish().await?;
                        Ok::<(), OsmPbfParquetError>(())
                    },
                    &handle,
                );
            } else {
                // Retain, still being written to
                pool.push(sink);
            }
        }
    }
    while let Some(result) = join_set.join_next().await {
        result??;
    }
    Ok(())
}

fn get_sink_from_pool(
    osm_type: OSMType,
    sinkpools: &SinkpoolStore,
    filenums: &HashMap<OSMType, Arc<Mutex<u64>>>,
) -> OsmPbfParquetResult<ElementSink> {
    {
        let mut pool = sinkpools[&osm_type]
            .lock()
            .expect("sinkpool mutex lock failed");
        if let Some(sink) = pool.pop() {
            return Ok(sink);
        }
    }
    ElementSink::new(filenums[&osm_type].clone(), osm_type)
}

fn add_sink_to_pool(sink: ElementSink, sinkpools: &SinkpoolStore) {
    let osm_type = sink.osm_type.clone();
    let mut pool = sinkpools[&osm_type]
        .lock()
        .expect("sinkpool mutex lock failed");
    pool.push(sink);
}

async fn process_block(
    block: PrimitiveBlock,
    sinkpools: Arc<SinkpoolStore>,
    filenums: Arc<HashMap<OSMType, Arc<Mutex<u64>>>>,
) -> OsmPbfParquetResult<u64> {
    let mut node_sink = get_sink_from_pool(OSMType::Node, &sinkpools, &filenums)?;
    let mut way_sink = get_sink_from_pool(OSMType::Way, &sinkpools, &filenums)?;
    let mut rel_sink = get_sink_from_pool(OSMType::Relation, &sinkpools, &filenums)?;

    let mut block_counter = 0u64;
    for element in block.elements() {
        block_counter += 1;
        match element {
            Element::Node(ref node) => {
                node_sink.add_node(node);
            }
            Element::DenseNode(ref node) => {
                node_sink.add_dense_node(node);
            }
            Element::Way(ref way) => {
                way_sink.add_way(way);
            }
            Element::Relation(ref rel) => {
                rel_sink.add_relation(rel);
            }
        }
    }
    ELEMENT_COUNTER.fetch_add(block_counter, std::sync::atomic::Ordering::Relaxed);

    node_sink.increment_and_cycle().await?;
    way_sink.increment_and_cycle().await?;
    rel_sink.increment_and_cycle().await?;
    add_sink_to_pool(node_sink, &sinkpools);
    add_sink_to_pool(way_sink, &sinkpools);
    add_sink_to_pool(rel_sink, &sinkpools);

    Ok(block_counter)
}
