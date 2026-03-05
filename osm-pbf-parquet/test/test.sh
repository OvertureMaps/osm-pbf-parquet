#!/bin/sh
set -e

# Check dependencies
for cmd in osmium cargo curl uv; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Error: '$cmd' is required but not found" >&2
        exit 1
    fi
done

# Cleanup old artifacts if exist
rm -rf ./parquet/**/*.parquet
mkdir -p ./parquet/

test_file="test"

# Download PBF, convert to OSM XML
if [ ! -f "./${test_file}.osm.pbf" ]; then
    echo "Downloading file"
    curl -L "https://download.geofabrik.de/australia-oceania/cook-islands-latest.osm.pbf" -o "${test_file}.osm.pbf"
    echo "Creating OSM XML"
    osmium cat "${test_file}.osm.pbf" -o "${test_file}.osm"
fi

# Run parquet conversion
echo "Running conversion"
cargo run --release -- --input "${test_file}.osm.pbf" --output ./parquet/

echo "Running validation"
uv run ./validate.py
