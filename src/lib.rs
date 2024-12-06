use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
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
use crate::util::{Args, SinkpoolStore, ARGS};

pub async fn pbf_driver(args: Args) -> Result<(), anyhow::Error> {
    // TODO - validation of args
    // Store value for reading across threads (write-once)
    let _ = ARGS.set(args.clone());

    let sinkpools: Arc<SinkpoolStore> = Arc::new(HashMap::from([
        (OSMType::Node, Arc::new(Mutex::new(vec![]))),
        (OSMType::Way, Arc::new(Mutex::new(vec![]))),
        (OSMType::Relation, Arc::new(Mutex::new(vec![]))),
    ]));

    // Verify we're running in a tokio runtime and start separate monitoring thread
    let sinkpool_monitor = sinkpools.clone();
    Handle::current().spawn(async move { monitor(sinkpool_monitor).await });

    let full_path = args.input;
    let buf_reader = if let Ok(url) = Url::parse(&full_path) {
        create_s3_buf_reader(url).await?
    } else {
        create_local_buf_reader(&full_path).await?
    };
    process_blobs(buf_reader, sinkpools.clone()).await?;

    finish_sinks(sinkpools.clone(), true).await?;

    Ok(())
}
