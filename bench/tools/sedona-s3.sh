#!/bin/bash
# Wrapper: Apache Sedona single-node (Spark local mode) osmpbf reader,
# writing zstd parquet to S3 (local MinIO) via s3a://.
# Usage: sedona-s3.sh <input.osm.pbf> <s3://bucket/prefix> <workers>
# (an s3a:// output URL is also accepted; s3:// is rewritten to s3a://)
# Env: JAVA_HOME; optional SPARK_DRIVER_MEMORY, SEDONA_PACKAGE,
# HADOOP_AWS_PACKAGE, BENCH_S3_ENDPOINT, BENCH_S3_ACCESS_KEY,
# BENCH_S3_SECRET_KEY, BENCH_S3_REGION (see bench/README.md).
# Requires uv (fetches pinned pyspark/sedona on first run; the first S3 run
# also fetches hadoop-aws + AWS SDK via ivy). Assumes MinIO is already up.
set -euo pipefail
INPUT="$1"; OUTURL="$2"; WORKERS="$3"

case "$OUTURL" in
    s3a://*) ;;
    s3://*) OUTURL="s3a://${OUTURL#s3://}" ;;
    *) echo "output must be an s3:// or s3a:// URL, got: $OUTURL" >&2; exit 2 ;;
esac

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-${TMPDIR:-/tmp}}"

exec uv run --quiet \
    --with "apache-sedona[spark]==1.9.0" \
    --with "pyspark==4.0.1" \
    --with pandas \
    python "$DIR/sedona_job.py" "$INPUT" "$OUTURL" "$WORKERS"
