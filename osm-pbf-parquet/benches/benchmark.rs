use criterion::{Criterion, criterion_group, criterion_main};
use osm_pbf_parquet::pbf_driver;
use osm_pbf_parquet::util::Args;
use std::fs;
use std::path::Path;

const TEST_FILE: &str = "./test/test.osm.pbf";

async fn bench() {
    let args = Args::new(TEST_FILE.to_string(), "./test/bench-out/".to_string(), 0);
    pbf_driver(args).await.unwrap();
}

pub fn criterion_benchmark(c: &mut Criterion) {
    if !Path::new(TEST_FILE).exists() {
        eprintln!(
            "Skipping benchmark: {} not found. Run ./test/test.sh first.",
            TEST_FILE
        );
        return;
    }
    c.bench_function("benchmark", |b| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        b.to_async(rt).iter(bench)
    });
    let _ = fs::remove_dir_all("./test/bench-out/");
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
