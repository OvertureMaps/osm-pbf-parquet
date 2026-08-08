#!/bin/bash
# Wrapper: DuckDB spatial ST_ReadOSM -> zstd parquet partitioned by kind,
# written to S3 (local MinIO) via the httpfs extension.
# Usage: duckdb-s3.sh <input.osm.pbf> <s3://bucket/prefix> <workers>
# Env: DUCKDB_BIN (default: duckdb on PATH); BENCH_S3_ENDPOINT,
# BENCH_S3_ACCESS_KEY, BENCH_S3_SECRET_KEY, BENCH_S3_REGION override the
# MinIO defaults (see bench/README.md). Assumes the endpoint is already up.
# Note: DuckDB errors if the target prefix already contains files; clear it
# before re-running (run.sh's rm -rf equivalent, e.g. mc rm -r --force).
set -euo pipefail
INPUT="$1"; OUTURL="$2"; WORKERS="$3"

case "$OUTURL" in
    s3://*) ;;
    *) echo "output must be an s3:// URL, got: $OUTURL" >&2; exit 2 ;;
esac

BIN="${DUCKDB_BIN:-duckdb}"
S3_ENDPOINT="${BENCH_S3_ENDPOINT:-http://127.0.0.1:9102}"
S3_ENDPOINT="${S3_ENDPOINT#http://}"   # DuckDB ENDPOINT wants host:port
S3_ACCESS_KEY="${BENCH_S3_ACCESS_KEY:-bench}"
S3_SECRET_KEY="${BENCH_S3_SECRET_KEY:-benchsecret123}"
S3_REGION="${BENCH_S3_REGION:-us-east-1}"

exec "$BIN" -c "
INSTALL spatial; LOAD spatial;
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3,
    KEY_ID '$S3_ACCESS_KEY',
    SECRET '$S3_SECRET_KEY',
    REGION '$S3_REGION',
    ENDPOINT '$S3_ENDPOINT',
    URL_STYLE 'path',
    USE_SSL false
);
SET threads=$WORKERS; SET memory_limit='24GB'; SET preserve_insertion_order=false;
COPY (SELECT * FROM ST_ReadOSM('$INPUT'))
TO '$OUTURL' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, PARTITION_BY (kind));
"
