#!/bin/bash
# Wrapper: osm-pbf-parquet. Env: OSM_PBF_PARQUET_BIN (default: build from this repo)
set -euo pipefail
INPUT="$1"; OUTDIR="$2"; WORKERS="$3"

BIN="${OSM_PBF_PARQUET_BIN:-}"
if [[ -z "$BIN" ]]; then
    REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    cargo build --release --quiet -p osm-pbf-parquet --manifest-path "$REPO/Cargo.toml"
    BIN="$REPO/target/release/osm-pbf-parquet"
fi

exec "$BIN" --input "$INPUT" --output "$OUTDIR" --worker-threads "$WORKERS"
