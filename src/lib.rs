use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use url::Url;

pub mod osm_arrow;
pub mod pbf;
pub mod sink;
pub mod util;
use crate::osm_arrow::OSMType;
use crate::pbf::{
    create_local_buf_reader, create_s3_buf_reader, finish_sinks, monitor, process_blobs,
};
use crate::sink::ElementSink;
use crate::util::{ARGS, Args, SinkpoolStore};

pub async fn pbf_driver(args: Args) -> Result<(), anyhow::Error> {
    // TODO - validation of args
    // Store value for reading across threads (write-once)
    let _ = ARGS.set(args.clone());

    let sinkpools: Arc<SinkpoolStore> = Arc::new(HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(vec![]))),
        (OSMType::Way, Arc::new(Mutex::new(vec![]))),
        (OSMType::Relation, Arc::new(Mutex::new(vec![]))),
    ]));

    // Start separate monitoring task with cancellation support
    let cancel_token = CancellationToken::new();
    let monitor_token = cancel_token.clone();
    let sinkpool_monitor = sinkpools.clone();
    let monitor_handle =
        Handle::current().spawn(async move { monitor(sinkpool_monitor, monitor_token).await });

    let full_path = args.input;
    let buf_reader = if let Ok(url) = Url::parse(&full_path) {
        create_s3_buf_reader(url).await?
    } else {
        create_local_buf_reader(&full_path).await?
    };
    process_blobs(buf_reader, sinkpools.clone()).await?;

    // Cancel monitor and wait for it to stop before final cleanup
    cancel_token.cancel();
    let _ = monitor_handle.await;

    finish_sinks(sinkpools.clone(), true).await?;

    Ok(())
}
