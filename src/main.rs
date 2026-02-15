use clap::Parser;
use env_logger::{Builder, Env};

use osm_pbf_parquet::OsmPbfParquetResult;
use osm_pbf_parquet::pbf_driver;
use osm_pbf_parquet::util::Args;

fn main() -> OsmPbfParquetResult<()> {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    println!("{:?}", args);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.get_worker_threads())
        .enable_all()
        .build()?;

    runtime.block_on(async { pbf_driver(args).await })
}
