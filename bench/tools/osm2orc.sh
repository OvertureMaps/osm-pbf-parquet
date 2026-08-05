#!/bin/bash
# Wrapper: osm2orc (https://github.com/mojodna/osm2orc)
# Env: JAVA_HOME, OSM2ORC_BIN (path to build/install/osm2orc/bin/osm2orc)
# Single ORC output file; thread count is not configurable.
set -euo pipefail
INPUT="$1"; OUTDIR="$2"; WORKERS="$3"

[[ -n "${OSM2ORC_BIN:-}" ]] || { echo "OSM2ORC_BIN not set" >&2; exit 2; }
export JAVA_HOME="${JAVA_HOME:-}"
exec "$OSM2ORC_BIN" "$INPUT" "$OUTDIR/planet.orc"
