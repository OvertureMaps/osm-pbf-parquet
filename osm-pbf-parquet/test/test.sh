#!/bin/sh
set -e

# Check dependencies
for cmd in cargo uv; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Error: '$cmd' is required but not found" >&2
        exit 1
    fi
done

# Cleanup old artifacts if exist
rm -rf ./parquet/**/*.parquet
mkdir -p ./parquet/

test_file="test"
fixture_pbf="../tests/fixtures/golden.osm.pbf"
fixture_osm="../tests/fixtures/golden.osm"

# Prepare test inputs
if [ ! -f "./${test_file}.osm.pbf" ] || [ ! -f "./${test_file}.osm" ]; then
    if [ -f "$fixture_pbf" ] && [ -f "$fixture_osm" ]; then
        echo "Using bundled fixture"
        cp "$fixture_pbf" "${test_file}.osm.pbf"
        cp "$fixture_osm" "${test_file}.osm"
    else
        for cmd in osmium curl; do
            if ! command -v "$cmd" >/dev/null 2>&1; then
                echo "Error: '$cmd' is required but not found" >&2
                exit 1
            fi
        done

        rm -f "${test_file}.osm.pbf" "${test_file}.osm"
        echo "Downloading file"
        curl --fail --show-error --location --retry 3 --retry-all-errors \
            "https://download.geofabrik.de/australia-oceania/cook-islands-latest.osm.pbf" \
            --output "${test_file}.osm.pbf"
        echo "Creating OSM XML"
        osmium cat "${test_file}.osm.pbf" -o "${test_file}.osm"
    fi
fi

# Run parquet conversion
echo "Running conversion"
cargo run --release -- --input "${test_file}.osm.pbf" --output ./parquet/

echo "Running validation"
uv run ./validate.py
