#!/bin/bash
# Wrapper: osm-parquetizer (https://github.com/adrianulbona/osm-parquetizer)
# Env: JAVA_HOME, PARQUETIZER_JAR (path to shaded osm-parquetizer jar)
# Writes <input>.node.parquet/.way.parquet/.relation.parquet next to the
# input, so we run it on a symlink inside OUTDIR. Thread count is not
# configurable.
set -euo pipefail
INPUT="$1"; OUTDIR="$2"; WORKERS="$3"

[[ -n "${PARQUETIZER_JAR:-}" ]] || { echo "PARQUETIZER_JAR not set" >&2; exit 2; }
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"

ln -sf "$INPUT" "$OUTDIR/input.osm.pbf"
"$JAVA" -jar "$PARQUETIZER_JAR" "$OUTDIR/input.osm.pbf"
rm -f "$OUTDIR/input.osm.pbf"
