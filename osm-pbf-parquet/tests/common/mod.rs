//! Synthetic PBF generation for tests that need more data than the golden
//! fixtures carry — file rotation, for one, cannot be reached by a 650 byte
//! fixture at the smallest legal `--file-target-mb`.
//!
//! Output is written fresh into `CARGO_TARGET_TMPDIR` on every run, so no
//! binary fixture is committed and the size is a parameter rather than a
//! property of a checked-in file.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use osmpbf::{fileformat, osmformat};
use protobuf::Message;

/// Elements per `PrimitiveBlock`, matching what real PBF writers emit.
const NODES_PER_BLOCK: u64 = 8000;

/// Deterministic PRNG. Coordinates need genuine entropy: sequential values
/// would delta-encode to almost nothing, so a compressed run would not
/// reproduce the estimate-vs-actual size gap that file rotation depends on.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

/// Writes `node_count` dense nodes with ids `1..=node_count` and pseudo-random
/// coordinates. Tags and metadata are omitted: `keys_vals` may be empty (the
/// decoder's tag loop simply yields nothing) and `DenseInfo` is optional.
///
/// Returns the path to the generated `.osm.pbf`.
pub fn write_dense_node_pbf(label: &str, node_count: u64) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{label}.osm.pbf"));
    let file = File::create(&path).expect("failed to create fixture");
    let mut out = BufWriter::new(file);

    write_blob(&mut out, "OSMHeader", header_block());
    let mut first_id = 1u64;
    while first_id <= node_count {
        let count = NODES_PER_BLOCK.min(node_count - first_id + 1);
        write_blob(&mut out, "OSMData", dense_block(first_id, count));
        first_id += count;
    }

    out.flush().expect("failed to flush fixture");
    path
}

fn header_block() -> Vec<u8> {
    let mut block = osmformat::HeaderBlock::new();
    block.required_features.push("OsmSchema-V0.6".into());
    block.required_features.push("DenseNodes".into());
    block
        .write_to_bytes()
        .expect("failed to encode HeaderBlock")
}

fn dense_block(first_id: u64, count: u64) -> Vec<u8> {
    // Seeded per block so the file is reproducible regardless of block size.
    let mut rng = Lcg(first_id.wrapping_mul(0x9E37_79B9_7F4A_7C15));

    let mut dense = osmformat::DenseNodes::new();
    let (mut prev_id, mut prev_lat, mut prev_lon) = (0i64, 0i64, 0i64);
    for offset in 0..count {
        let id = (first_id + offset) as i64;
        // granularity 100 => degrees = value * 1e-7
        let lat = (rng.next() % 1_700_000_000) as i64 - 850_000_000;
        let lon = (rng.next() % 3_600_000_000) as i64 - 1_800_000_000;

        dense.id.push(id - prev_id);
        dense.lat.push(lat - prev_lat);
        dense.lon.push(lon - prev_lon);
        (prev_id, prev_lat, prev_lon) = (id, lat, lon);
    }

    let mut group = osmformat::PrimitiveGroup::new();
    group.dense = protobuf::MessageField::some(dense);

    // Index 0 of the string table is required to be empty.
    let mut stringtable = osmformat::StringTable::new();
    stringtable.s.push(Vec::new().into());

    let mut block = osmformat::PrimitiveBlock::new();
    block.stringtable = protobuf::MessageField::some(stringtable);
    block.primitivegroup.push(group);
    block.set_granularity(100);
    block
        .write_to_bytes()
        .expect("failed to encode PrimitiveBlock")
}

/// A PBF is a sequence of (big-endian u32 header length, BlobHeader, Blob).
/// Blobs are written uncompressed via the `raw` field, which the reader
/// accepts directly.
fn write_blob<W: Write>(out: &mut W, blob_type: &str, payload: Vec<u8>) {
    let mut blob = fileformat::Blob::new();
    blob.set_raw(payload.into());
    let blob_bytes = blob.write_to_bytes().expect("failed to encode Blob");

    let mut header = fileformat::BlobHeader::new();
    header.set_type(blob_type.into());
    header.set_datasize(blob_bytes.len() as i32);
    let header_bytes = header
        .write_to_bytes()
        .expect("failed to encode BlobHeader");

    out.write_all(&(header_bytes.len() as u32).to_be_bytes())
        .expect("failed to write blob header length");
    out.write_all(&header_bytes)
        .expect("failed to write header");
    out.write_all(&blob_bytes).expect("failed to write blob");
}
