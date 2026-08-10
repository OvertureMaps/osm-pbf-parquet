#!/bin/bash
# Wrapper: Apache Sedona single-node (Spark local mode) osmpbf reader.
# Env: JAVA_HOME; optional SPARK_DRIVER_MEMORY, SEDONA_PACKAGE.
# Requires uv (fetches pinned pyspark/sedona on first run).
# Full-metadata schema, directly comparable to osm-pbf-parquet.
set -euo pipefail
INPUT="$1"; OUTDIR="$2"; WORKERS="$3"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-${TMPDIR:-/tmp}}"

exec uv run --quiet \
    --with "apache-sedona[spark]==1.9.0" \
    --with "pyspark==4.0.1" \
    --with pandas \
    python "$DIR/sedona_job.py" "$INPUT" "$OUTDIR" "$WORKERS"
