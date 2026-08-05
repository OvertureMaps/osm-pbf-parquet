#!/bin/bash
# Wrapper: DuckDB spatial ST_ReadOSM -> zstd parquet partitioned by kind.
# Env: DUCKDB_BIN (default: duckdb on PATH)
# Note: ST_ReadOSM omits metadata columns (changeset, timestamp, uid, user,
# version, visible) — it does less work than the other tools.
set -euo pipefail
INPUT="$1"; OUTDIR="$2"; WORKERS="$3"

BIN="${DUCKDB_BIN:-duckdb}"
exec "$BIN" -c "
INSTALL spatial; LOAD spatial;
SET threads=$WORKERS; SET memory_limit='24GB'; SET preserve_insertion_order=false;
COPY (SELECT * FROM ST_ReadOSM('$INPUT'))
TO '$OUTDIR' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, PARTITION_BY (kind));
"
