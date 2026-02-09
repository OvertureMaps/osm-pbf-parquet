use criterion::{Criterion, criterion_group, criterion_main};
use osm_pbf_parquet::pbf_driver;
use osm_pbf_parquet::util::Args;
use std::fs;

async fn bench() {
    let args = Args::new(
        "./test/test.osm.pbf".to_string(),
        "./test/bench-out/".to_string(),
        0,
    );
    pbf_driver(args).await.unwrap();
}

pub fn criterion_benchmark(c: &mut Criterion) {
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
