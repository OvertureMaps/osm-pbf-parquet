//! File rotation round-trip.
//!
//! Rotation is the one write path the golden fixtures cannot reach — they are
//! a few hundred bytes, and the smallest legal `--file-target-mb` is 1. These
//! tests generate enough synthetic input to cross that boundary several times
//! and assert nothing is lost or duplicated at the seams.

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const NODE_COUNT: u64 = 300_000;
/// Enough that every sink in a multi-worker pool rotates at least once.
const CONCURRENT_NODE_COUNT: u64 = 900_000;
const ROW_GROUP_ROWS: &str = "20000";

fn convert(input: &Path, label: &str, workers: u32, extra_args: &[&str]) -> PathBuf {
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(label);
    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir).expect("failed to clear output dir");
    }
    std::fs::create_dir_all(&out_dir).expect("failed to create output dir");

    let status = Command::new(env!("CARGO_BIN_EXE_osm-pbf-parquet"))
        .arg("--input")
        .arg(input)
        .arg("--output")
        .arg(&out_dir)
        .arg("--worker-threads")
        .arg(workers.to_string())
        .args(extra_args)
        .status()
        .expect("failed to run osm-pbf-parquet binary");
    assert!(status.success(), "conversion exited with {status}");
    out_dir
}

fn node_files(out_dir: &Path) -> Vec<PathBuf> {
    let type_dir = out_dir.join("type=node");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&type_dir)
        .unwrap_or_else(|e| panic!("missing output dir {}: {e}", type_dir.display()))
        .map(|entry| entry.expect("failed to read dir entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    paths.sort();
    paths
}

/// Every id read back across all files, in the order encountered. Returned as
/// a Vec rather than a set so duplicates are still visible to the caller.
fn collect_ids(paths: &[PathBuf]) -> Vec<i64> {
    let mut ids = Vec::new();
    for path in paths {
        let file = std::fs::File::open(path).expect("failed to open parquet file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("failed to open parquet reader")
            .build()
            .expect("failed to build parquet reader");
        for batch in reader {
            let batch = batch.expect("failed to read batch");
            let column = batch
                .column_by_name("id")
                .expect("missing id column")
                .as_primitive::<Int64Type>();
            ids.extend(column.iter().map(|v| v.expect("null id")));
        }
    }
    ids
}

/// One worker means one sink in the pool, so every file past the first is
/// necessarily a rotation rather than a second sink writing in parallel.
#[test]
fn rotation_preserves_every_row() {
    let input = common::write_dense_node_pbf("rotation-nodes", NODE_COUNT);
    let out_dir = convert(
        &input,
        "rotation-preserves",
        1,
        &[
            "--compression",
            "0",
            "--file-target-mb",
            "1",
            "--record-batch-target-mb",
            "1",
        ],
    );

    let paths = node_files(&out_dir);
    assert!(
        paths.len() > 1,
        "expected rotation to produce multiple files, got {}",
        paths.len()
    );
    assert_ids_complete(&paths, NODE_COUNT);
}

/// The pool holds up to `1.5 * worker_threads` sinks, each owning its own file,
/// so a file count above that bound is what proves rotation happened rather
/// than sinks merely running in parallel. Concurrency is where rows would be
/// lost or duplicated if a rotation raced the writes around it.
#[test]
fn concurrent_sinks_preserve_every_row() {
    const WORKERS: u32 = 4;
    let max_sinks = (1.5 * WORKERS as f32) as usize;

    let input = common::write_dense_node_pbf("rotation-concurrent-nodes", CONCURRENT_NODE_COUNT);
    let out_dir = convert(
        &input,
        "rotation-concurrent",
        WORKERS,
        &[
            "--compression",
            "3",
            "--file-target-mb",
            "1",
            "--record-batch-target-mb",
            "1",
        ],
    );

    let paths = node_files(&out_dir);
    assert!(
        paths.len() > max_sinks,
        "got {} files for at most {max_sinks} sinks, so nothing necessarily \
         rotated — raise CONCURRENT_NODE_COUNT",
        paths.len()
    );
    assert_ids_complete(&paths, CONCURRENT_NODE_COUNT);
}

/// Ids are generated as `1..=expected`, so a complete, duplicate-free read back
/// across every output file is exactly that range.
fn assert_ids_complete(paths: &[PathBuf], expected: u64) {
    let ids = collect_ids(paths);
    assert_eq!(
        ids.len(),
        expected as usize,
        "row count across {} files does not match input",
        paths.len()
    );

    let unique: BTreeSet<i64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "ids duplicated across files");
    assert_eq!(*unique.first().expect("no ids"), 1);
    assert_eq!(*unique.last().expect("no ids"), expected as i64);
}

/// Rotation must be driven by what the writer actually emitted. Accumulating
/// the per-element size estimate instead closed files several times under
/// target, which this catches.
///
/// Measured on this fixture at a 1MB target: writer-reported bytes land the
/// rotated files at ~1078KB, the old estimate landed them at ~362KB. The bar
/// below sits between the two with room for compression to vary.
#[test]
fn rotated_files_reach_their_target() {
    let paths = rotate_at_1mb("rotation-target", &[]);
    assert_rotated_files_reach_target(&paths);
}

/// The rotation check sums two terms: bytes already flushed to the sink, and
/// the encoded size of the row group still buffered. At parquet's default of
/// ~1M rows per row group these fixtures fit in a single row group, leaving
/// `bytes_written()` at just the file magic — so the test above only covers
/// the buffered term. Production runs the opposite regime (~29 row groups per
/// 500MB file), so cap the row group size here to flush several per file and
/// put the weight on the flushed term instead.
#[test]
fn rotation_accounts_for_flushed_row_groups() {
    let paths = rotate_at_1mb(
        "rotation-row-groups",
        &["--max-row-group-count", ROW_GROUP_ROWS],
    );

    let row_groups = row_group_count(&paths[0]);
    assert!(
        row_groups > 1,
        "expected several row groups per file, got {row_groups} — this test no \
         longer covers the flushed-bytes term"
    );

    assert_rotated_files_reach_target(&paths);
}

/// `label` also names the fixture: tests run in parallel and `File::create`
/// truncates, so two of them must not share an input path.
fn rotate_at_1mb(label: &str, extra: &[&str]) -> Vec<PathBuf> {
    let input = common::write_dense_node_pbf(&format!("{label}-input"), NODE_COUNT);
    let mut args = vec![
        "--compression",
        "3",
        "--file-target-mb",
        "1",
        "--record-batch-target-mb",
        "1",
    ];
    args.extend_from_slice(extra);
    let out_dir = convert(&input, label, 1, &args);

    let paths = node_files(&out_dir);
    assert!(
        paths.len() > 1,
        "expected rotation to produce multiple files, got {}",
        paths.len()
    );
    paths
}

fn assert_rotated_files_reach_target(paths: &[PathBuf]) {
    // The final file holds whatever remained when input ran out, so only the
    // rotated ones carry a size guarantee.
    for path in &paths[..paths.len() - 1] {
        let size = std::fs::metadata(path).expect("failed to stat file").len();
        assert!(
            size >= 768 * 1024,
            "{} is {size} bytes, well under the 1MB target",
            path.display()
        );
    }
}

fn row_group_count(path: &Path) -> usize {
    let file = std::fs::File::open(path).expect("failed to open parquet file");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to open parquet reader")
        .metadata()
        .num_row_groups()
}
