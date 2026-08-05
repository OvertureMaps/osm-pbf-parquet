#!/bin/bash
# Multi-tool OSM PBF transcode benchmark driver.
#
# Usage:
#   bench/run.sh --input /path/to/x.osm.pbf --output /scratch/bench-out \
#       [--tools osm-pbf-parquet,duckdb,osm-parquetizer,osm2orc] \
#       [--workers 8] [--nice 10] [--label mylabel] [--validate]
#
# Tool locations are read from bench/config.env (see config.env.example).
# Results land in bench/results/<label>/: per-tool time+iostat logs and
# summary.tsv. Each tool writes to <output>/<tool>/.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS="osm-pbf-parquet,duckdb,osm-parquetizer,osm2orc"
WORKERS=8
NICENESS=10
LABEL="$(date +%Y%m%d-%H%M%S)"
INPUT="" OUTPUT="" VALIDATE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --input) INPUT="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        --tools) TOOLS="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --nice) NICENESS="$2"; shift 2 ;;
        --label) LABEL="$2"; shift 2 ;;
        --validate) VALIDATE=1; shift ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done
[[ -n "$INPUT" && -n "$OUTPUT" ]] || { echo "--input and --output are required" >&2; exit 2; }
[[ -f "$INPUT" ]] || { echo "input not found: $INPUT" >&2; exit 2; }

[[ -f "$BENCH_DIR/config.env" ]] && source "$BENCH_DIR/config.env"

RESULTS="$BENCH_DIR/results/$LABEL"
mkdir -p "$RESULTS" "$OUTPUT"

# Map a path to its parent block device for iostat (resolves partitions and
# LVM/dm to the physical parent where possible).
device_for_path() {
    local src dev
    src=$(df -P "$1" | awk 'NR==2 {print $1}')
    dev=$(lsblk -no pkname "$src" 2>/dev/null | head -1)
    if [[ -z "$dev" ]]; then
        dev=$(basename "$src")
    fi
    echo "$dev"
}

IN_DEV=$(device_for_path "$(dirname "$INPUT")")
OUT_DEV=$(device_for_path "$OUTPUT")
echo "input device: $IN_DEV, output device: $OUT_DEV"

SUMMARY="$RESULTS/summary.tsv"
echo -e "tool\twall_s\tuser_s\tsys_s\tcpu_pct\tmax_rss_mb\tout_bytes\tout_files\tin_dev_r_mbps\tout_dev_w_mbps" > "$SUMMARY"

extract_time_stat() { # file, label
    grep -F "$2" "$1" | head -1 | sed 's/.*: //'
}

hms_to_s() { # h:mm:ss or m:ss.xx -> seconds
    awk -F: '{ if (NF==3) print $1*3600+$2*60+$3; else print $1*60+$2 }' <<<"$1"
}

for tool in ${TOOLS//,/ }; do
    wrapper="$BENCH_DIR/tools/$tool.sh"
    [[ -x "$wrapper" ]] || { echo "SKIP $tool: no wrapper $wrapper" >&2; continue; }
    outdir="$OUTPUT/$tool"
    rm -rf "$outdir"; mkdir -p "$outdir"

    echo "=== $tool (workers=$WORKERS nice=$NICENESS) ==="
    iostat -x 5 -d "$IN_DEV" "$OUT_DEV" > "$RESULTS/$tool.iostat" 2>&1 &
    IOSTAT_PID=$!

    set +e
    /usr/bin/time -v nice -n "$NICENESS" \
        "$wrapper" "$INPUT" "$outdir" "$WORKERS" > "$RESULTS/$tool.log" 2>&1
    rc=$?
    set -e
    kill "$IOSTAT_PID" 2>/dev/null || true
    wait "$IOSTAT_PID" 2>/dev/null || true

    if [[ $rc -ne 0 ]]; then
        echo "FAIL $tool rc=$rc (see $RESULTS/$tool.log)"
        echo -e "$tool\tFAIL\t-\t-\t-\t-\t-\t-\t-\t-" >> "$SUMMARY"
        continue
    fi

    wall=$(hms_to_s "$(extract_time_stat "$RESULTS/$tool.log" 'Elapsed (wall clock)')")
    user=$(extract_time_stat "$RESULTS/$tool.log" 'User time (seconds)')
    sys=$(extract_time_stat "$RESULTS/$tool.log" 'System time (seconds)')
    cpu=$(extract_time_stat "$RESULTS/$tool.log" 'Percent of CPU' | tr -d '%')
    rss_kb=$(extract_time_stat "$RESULTS/$tool.log" 'Maximum resident set size')
    out_bytes=$(du -sb "$outdir" | cut -f1)
    out_files=$(find "$outdir" -type f | wc -l)
    # iostat: column 3 is rkB/s, column 9 is wkB/s (sysstat 12.x -x layout)
    in_r=$(awk -v d="$IN_DEV" '$1==d {n++; s+=$3} END {if (n) printf "%.0f", s/n/1024; else print "-"}' "$RESULTS/$tool.iostat")
    out_w=$(awk -v d="$OUT_DEV" '$1==d {n++; s+=$9} END {if (n) printf "%.0f", s/n/1024; else print "-"}' "$RESULTS/$tool.iostat")

    echo -e "$tool\t$wall\t$user\t$sys\t$cpu\t$((rss_kb / 1024))\t$out_bytes\t$out_files\t$in_r\t$out_w" >> "$SUMMARY"
    column -t "$SUMMARY" | tail -1
done

echo
echo "=== summary ($RESULTS/summary.tsv) ==="
column -t "$SUMMARY"

if [[ $VALIDATE -eq 1 ]]; then
    echo
    echo "=== accuracy comparison ==="
    "$BENCH_DIR/validate.py" --base "$OUTPUT" --tools "$TOOLS" | tee "$RESULTS/validation.txt"
fi
