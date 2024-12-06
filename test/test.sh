#!/bin/sh
set -e

# Cleanup old artifacts if exist
rm -rf ./parquet/**/*.parquet
mkdir -p ./parquet/

test_file="test"

# Download PBF, convert to OSM XML
if [ ! -f "./${test_file}.osm.pbf" ]; then
    echo "Downloading file"
    curl -L "https://download.geofabrik.de/australia-oceania/cook-islands-latest.osm.pbf" -o "${test_file}.osm.pbf"
    echo "Creating OSM XML"
    osmium cat ${test_file}.osm.pbf -o ${test_file}.osm
fi

# Run parquet conversion
echo "Running conversion"
cargo run --release -- --input ${test_file}.osm.pbf --output ./parquet/

echo "Running validation"
python3 ./validate.py

# Cleanup artifacts
# rm -rf ./parquet/**/*.parquet
