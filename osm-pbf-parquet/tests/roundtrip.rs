//! Golden-fixture round-trip tests.
//!
//! Converts `tests/fixtures/golden.osm.pbf` (see `golden.osm` for the
//! human-readable source) with the real binary and asserts the full content
//! and exact schema of the parquet output. The expectations are hardcoded so
//! any behavior change — dropped metadata, reordered refs, renamed nested
//! fields — fails loudly, including changes introduced by dependency
//! upgrades that per-row comparisons through pandas would normalize away.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, TimestampMillisecondType};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const NODE_IDS: [i64; 5] = [101, 102, 103, 104, 105];
const WAY_IDS: [i64; 2] = [201, 202];
const RELATION_IDS: [i64; 2] = [301, 302];

#[derive(Debug, Clone, PartialEq)]
struct Row {
    id: i64,
    tags: Vec<(String, String)>,
    lat: Option<f64>,
    lon: Option<f64>,
    nds: Vec<i64>,
    members: Vec<(String, i64, String)>,
    changeset: Option<i64>,
    timestamp: Option<i64>,
    uid: Option<i32>,
    user: Option<String>,
    version: Option<i32>,
    visible: Option<bool>,
}

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn convert(fixture: &str, label: &str, extra_args: &[&str]) -> PathBuf {
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(label);
    if out_dir.exists() {
        std::fs::remove_dir_all(&out_dir).expect("failed to clear output dir");
    }
    std::fs::create_dir_all(&out_dir).expect("failed to create output dir");

    let status = Command::new(env!("CARGO_BIN_EXE_osm-pbf-parquet"))
        .arg("--input")
        .arg(fixture_path(fixture))
        .arg("--output")
        .arg(&out_dir)
        .arg("--worker-threads")
        .arg("2")
        .args(extra_args)
        .status()
        .expect("failed to run osm-pbf-parquet binary");
    assert!(status.success(), "conversion exited with {status}");
    out_dir
}

fn read_batches(out_dir: &Path, osm_type: &str) -> (Arc<Schema>, Vec<RecordBatch>) {
    let type_dir = out_dir.join(format!("type={osm_type}"));
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&type_dir)
        .unwrap_or_else(|e| panic!("missing output dir {}: {e}", type_dir.display()))
        .map(|entry| entry.expect("failed to read dir entry").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no parquet files in {}",
        type_dir.display()
    );

    let mut schema = None;
    let mut batches = Vec::new();
    for path in paths {
        let file = File::open(&path).expect("failed to open parquet file");
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("failed to read parquet");
        let file_schema = builder.schema().clone();
        if let Some(previous) = &schema {
            assert_eq!(
                previous, &file_schema,
                "schema differs between output files"
            );
        }
        schema = Some(file_schema);
        for batch in builder.build().expect("failed to build reader") {
            batches.push(batch.expect("failed to decode batch"));
        }
    }
    (schema.expect("no files read"), batches)
}

fn extract_rows(batches: &[RecordBatch]) -> BTreeMap<i64, Row> {
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let tags = batch.column_by_name("tags").unwrap().as_map();
        let tag_keys = tags.keys().as_string::<i32>();
        let tag_values = tags.values().as_string::<i32>();
        let lats = batch
            .column_by_name("lat")
            .unwrap()
            .as_primitive::<Float64Type>();
        let lons = batch
            .column_by_name("lon")
            .unwrap()
            .as_primitive::<Float64Type>();
        let nds = batch.column_by_name("nds").unwrap().as_list::<i32>();
        let members = batch.column_by_name("members").unwrap().as_list::<i32>();
        let changesets = batch
            .column_by_name("changeset")
            .unwrap()
            .as_primitive::<Int64Type>();
        let timestamps = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        let uids = batch
            .column_by_name("uid")
            .unwrap()
            .as_primitive::<Int32Type>();
        let users = batch.column_by_name("user").unwrap().as_string::<i32>();
        let versions = batch
            .column_by_name("version")
            .unwrap()
            .as_primitive::<Int32Type>();
        let visibles = batch.column_by_name("visible").unwrap().as_boolean();

        for i in 0..batch.num_rows() {
            let row_tags = if tags.is_null(i) {
                Vec::new()
            } else {
                let start = tags.value_offsets()[i] as usize;
                let end = tags.value_offsets()[i + 1] as usize;
                (start..end)
                    .map(|j| {
                        (
                            tag_keys.value(j).to_string(),
                            tag_values.value(j).to_string(),
                        )
                    })
                    .collect()
            };

            let row_nds = if nds.is_null(i) {
                Vec::new()
            } else {
                let value = nds.value(i);
                let structs = value.as_struct();
                let refs = structs
                    .column_by_name("ref")
                    .unwrap()
                    .as_primitive::<Int64Type>();
                (0..refs.len()).map(|j| refs.value(j)).collect()
            };

            let row_members = if members.is_null(i) {
                Vec::new()
            } else {
                let value = members.value(i);
                let structs = value.as_struct();
                let types = structs.column_by_name("type").unwrap().as_string::<i32>();
                let refs = structs
                    .column_by_name("ref")
                    .unwrap()
                    .as_primitive::<Int64Type>();
                let roles = structs.column_by_name("role").unwrap().as_string::<i32>();
                (0..structs.len())
                    .map(|j| {
                        (
                            types.value(j).to_string(),
                            refs.value(j),
                            roles.value(j).to_string(),
                        )
                    })
                    .collect()
            };

            let row = Row {
                id: ids.value(i),
                tags: row_tags,
                lat: (!lats.is_null(i)).then(|| lats.value(i)),
                lon: (!lons.is_null(i)).then(|| lons.value(i)),
                nds: row_nds,
                members: row_members,
                changeset: (!changesets.is_null(i)).then(|| changesets.value(i)),
                timestamp: (!timestamps.is_null(i)).then(|| timestamps.value(i)),
                uid: (!uids.is_null(i)).then(|| uids.value(i)),
                user: (!users.is_null(i)).then(|| users.value(i).to_string()),
                version: (!versions.is_null(i)).then(|| versions.value(i)),
                visible: (!visibles.is_null(i)).then(|| visibles.value(i)),
            };
            rows.insert(row.id, row);
        }
    }
    rows
}

fn assert_coord(actual: Option<f64>, expected: f64, what: &str) {
    let actual = actual.unwrap_or_else(|| panic!("{what}: expected coordinate, got null"));
    assert!(
        (actual - expected).abs() < 1e-9,
        "{what}: expected {expected}, got {actual}"
    );
}

fn tags(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn expected_schema() -> Schema {
    // Constructed independently from src/osm_arrow.rs on purpose: if a
    // dependency upgrade or refactor changes any nested field name or type,
    // this must fail even though the writer and its schema changed together.
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
        Field::new(
            "nds",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![Field::new("ref", DataType::Int64, true)])),
                true,
            ))),
            true,
        ),
        Field::new(
            "members",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::Utf8, true),
                    Field::new("ref", DataType::Int64, true),
                    Field::new("role", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        ),
        Field::new("changeset", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("uid", DataType::Int32, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("version", DataType::Int32, true),
        Field::new("visible", DataType::Boolean, true),
    ])
}

#[test]
fn dense_roundtrip_schema_and_content() {
    let out = convert("golden.osm.pbf", "golden-dense", &[]);

    let (schema, node_batches) = read_batches(&out, "node");
    let expected = expected_schema();
    for (actual_field, expected_field) in schema.fields().iter().zip(expected.fields()) {
        assert_eq!(
            (
                actual_field.name(),
                actual_field.data_type(),
                actual_field.is_nullable()
            ),
            (
                expected_field.name(),
                expected_field.data_type(),
                expected_field.is_nullable()
            ),
            "schema mismatch on field {}",
            expected_field.name()
        );
    }
    assert_eq!(schema.fields().len(), expected.fields().len());

    let nodes = extract_rows(&node_batches);
    let (_, way_batches) = read_batches(&out, "way");
    let ways = extract_rows(&way_batches);
    let (_, relation_batches) = read_batches(&out, "relation");
    let relations = extract_rows(&relation_batches);

    assert_eq!(nodes.keys().copied().collect::<Vec<_>>(), NODE_IDS);
    assert_eq!(ways.keys().copied().collect::<Vec<_>>(), WAY_IDS);
    assert_eq!(relations.keys().copied().collect::<Vec<_>>(), RELATION_IDS);

    // Nodes
    let n101 = &nodes[&101];
    assert_eq!(
        n101.tags,
        tags(&[("amenity", "cafe"), ("name", "Café München 🍴")])
    );
    assert_coord(n101.lat, 45.5, "node 101 lat");
    assert_coord(n101.lon, -122.25, "node 101 lon");
    assert!(n101.nds.is_empty() && n101.members.is_empty());
    assert_eq!(n101.changeset, Some(9001));
    assert_eq!(n101.timestamp, Some(1577934245000));
    assert_eq!(n101.uid, Some(42));
    assert_eq!(n101.user.as_deref(), Some("alice"));
    assert_eq!(n101.version, Some(3));
    assert_eq!(n101.visible, Some(true));

    let n102 = &nodes[&102];
    assert!(n102.tags.is_empty(), "node 102 should have no tags");
    assert_coord(n102.lat, -33.8688, "node 102 lat");
    assert_coord(n102.lon, 151.2093, "node 102 lon");
    assert_eq!(n102.changeset, Some(9002));
    assert_eq!(n102.timestamp, Some(1623053350000));
    assert_eq!(n102.uid, Some(7));
    assert_eq!(n102.user.as_deref(), Some("bøb"));
    assert_eq!(n102.version, Some(1));

    let n103 = &nodes[&103];
    assert_eq!(
        n103.tags,
        tags(&[("note", "")]),
        "empty tag value preserved"
    );
    assert_coord(n103.lat, 0.0000001, "node 103 lat");
    assert_coord(n103.lon, 0.0000001, "node 103 lon");
    assert_eq!(n103.timestamp, Some(1668258855000));

    assert_coord(nodes[&104].lat, 89.9999999, "node 104 lat");
    assert_coord(nodes[&104].lon, 179.9999999, "node 104 lon");
    assert_eq!(nodes[&104].timestamp, Some(1551675967000));
    assert_coord(nodes[&105].lat, -89.9999999, "node 105 lat");
    assert_coord(nodes[&105].lon, -179.9999999, "node 105 lon");
    assert_eq!(nodes[&105].timestamp, Some(1551675968000));

    // Ways
    let w201 = &ways[&201];
    assert_eq!(w201.nds, vec![101, 102, 103], "way 201 refs in order");
    assert_eq!(
        w201.tags,
        tags(&[("highway", "residential"), ("name", "Test Street")])
    );
    assert_eq!(w201.lat, None);
    assert_eq!(w201.lon, None);
    assert_eq!(w201.changeset, Some(9005));
    assert_eq!(w201.timestamp, Some(1588748889000));
    assert_eq!(w201.uid, Some(42));
    assert_eq!(w201.user.as_deref(), Some("alice"));
    assert_eq!(w201.version, Some(5));
    assert_eq!(w201.visible, Some(true));

    let w202 = &ways[&202];
    assert_eq!(w202.nds, vec![104, 105]);
    assert!(w202.tags.is_empty());
    assert_eq!(w202.timestamp, Some(1625735411000));

    // Relations
    let r301 = &relations[&301];
    assert_eq!(
        r301.members,
        vec![
            ("node".to_string(), 101, "stop".to_string()),
            ("way".to_string(), 201, String::new()),
            ("relation".to_string(), 302, "parent".to_string()),
        ],
        "relation 301 members in order, empty role preserved"
    );
    assert_eq!(r301.tags, tags(&[("type", "route"), ("route", "bus")]));
    assert!(r301.nds.is_empty());
    assert_eq!(r301.changeset, Some(9007));
    assert_eq!(r301.timestamp, Some(1662808333000));
    assert_eq!(r301.uid, Some(99));
    assert_eq!(r301.user.as_deref(), Some("carol"));
    assert_eq!(r301.version, Some(2));

    let r302 = &relations[&302];
    assert_eq!(
        r302.members,
        vec![("way".to_string(), 202, "outer".to_string())]
    );
    assert_eq!(r302.tags, tags(&[("type", "multipolygon")]));
    assert_eq!(r302.timestamp, Some(1672628645000));
}

#[test]
fn plain_nodes_match_dense_nodes() {
    // golden-plain.osm.pbf carries identical data encoded without dense
    // nodes, exercising the non-dense decode path; output must be identical.
    let dense_out = convert("golden.osm.pbf", "golden-dense-cmp", &[]);
    let plain_out = convert("golden-plain.osm.pbf", "golden-plain-cmp", &[]);

    for osm_type in ["node", "way", "relation"] {
        let (_, dense_batches) = read_batches(&dense_out, osm_type);
        let (_, plain_batches) = read_batches(&plain_out, osm_type);
        assert_eq!(
            extract_rows(&dense_batches),
            extract_rows(&plain_batches),
            "{osm_type} rows differ between dense and plain encoding"
        );
    }
}

#[test]
fn uncompressed_output_is_readable() {
    let out = convert(
        "golden.osm.pbf",
        "golden-uncompressed",
        &["--compression", "0"],
    );
    let node_dir = out.join("type=node");
    for entry in std::fs::read_dir(&node_dir).expect("missing node dir") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.contains(".zstd."),
            "uncompressed output should not be named .zstd.parquet: {name}"
        );
    }
    let (_, batches) = read_batches(&out, "node");
    assert_eq!(extract_rows(&batches).len(), 5);
}

#[test]
fn no_output_files_for_types_without_data() {
    // golden-nodes.osm.pbf contains only nodes; sinks for ways and
    // relations receive no data and must not emit empty parquet files.
    let out = convert("golden-nodes.osm.pbf", "golden-nodes-only", &[]);

    let (_, batches) = read_batches(&out, "node");
    assert_eq!(extract_rows(&batches).len(), 5);

    for osm_type in ["way", "relation"] {
        let type_dir = out.join(format!("type={osm_type}"));
        let files: Vec<String> = match std::fs::read_dir(&type_dir) {
            Err(_) => Vec::new(),
            Ok(entries) => entries
                .map(|entry| {
                    entry
                        .expect("failed to read dir entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect(),
        };
        assert!(
            files.is_empty(),
            "expected no {osm_type} output files, found {files:?}"
        );
    }
}
