# osm-pbf-parquet
Transcode OSM PBF file to parquet files with hive-style partitioning by type

## Getting started

### Download
Download latest version from [releases](https://github.com/OvertureMaps/osm-pbf-parquet/releases)

### Usage
Example for x86_64 linux system with pre-compiled binary:
```
curl -L "https://github.com/OvertureMaps/osm-pbf-parquet/releases/latest/download/osm-pbf-parquet-x86_64-unknown-linux-gnu.tar.gz" -o "osm-pbf-parquet.tar.gz"
tar -xzf osm-pbf-parquet.tar.gz
chmod +x osm-pbf-parquet
./osm-pbf-parquet --input your.osm.pbf --output ./parquet
```

OR compile and run locally:
```
git clone https://github.com/OvertureMaps/osm-pbf-parquet.git
cargo run --release -- --input your.osm.pbf --output ./parquet
```

### Supported input/output
- Local filesystem
- AWS S3 (auth read from environment, see [object_store docs](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html))

### Output structure
```
planet.osm.pbf
parquet/
  type=node/
    node_0000.zstd.parquet
    ...
  type=relation/
    relation_0000.zstd.parquet
    ...
  type=way/
    way_0000.zstd.parquet
    ...
```
[Reference Arrow/SQL schema](https://github.com/OvertureMaps/osm-pbf-parquet/blob/main/src/osm_arrow.rs)

### Querying

#### DuckDB
```
duckdb -c "SELECT * FROM read_parquet('s3://your-s3-bucket/path/') LIMIT 10;"
```

#### Athena/Presto/Trino
```
CREATE EXTERNAL TABLE IF NOT EXISTS `osm` (
    `id` BIGINT,
    `tags` MAP<STRING, STRING>,
    `lat` DOUBLE,
    `lon` DOUBLE,
    `nds` ARRAY<STRUCT<ref: BIGINT>>,
    `members` ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>,
    `changeset` BIGINT,
    `timestamp` TIMESTAMP,
    `uid` BIGINT,
    `user` STRING,
    `version` BIGINT,
    `visible` BOOLEAN
)
PARTITIONED BY (
    `type` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://your-s3-bucket/path/';

MSCK REPAIR TABLE `osm`;

SELECT * FROM osm LIMIT 10;
```

## Development
1. [Install rust](https://www.rust-lang.org/tools/install)
2. Clone repo `git clone https://github.com/OvertureMaps/osm-pbf-parquet.git`
3. Make changes
4. Run against PBF with `cargo run -- --input your.osm.pbf` ([Geofabrik regional PBF extracts here](https://download.geofabrik.de/))
5. Test with `cd test && ./prepare.sh && python3 validate.py`


## Benchmarks
osm-pbf-parquet prioritizes transcode speed over file size, file count or perserving ordering. Here is a comparison against similar tools on the 2024-06-24 OSM planet PBF with target file size of 500MB:
| | Time (wall) | Output size | File count |
| - | - | - | - |
| **osm-pbf-parquet** (zstd:3) | 30 minutes | 182GB | ~600 |
| **osm-pbf-parquet** (zstd:9) | 60 minutes | 165GB | ~600 |
| [osm-parquetizer](https://github.com/adrianulbona/osm-parquetizer) | 196 minutes | 285GB | 3 |
| [osm2orc](https://github.com/mojodna/osm2orc) | 385 minutes | 110GB | 1 |

Test system:
```
i5-9400 (6 CPU, 32GB memory)
Ubuntu 24.04
OpenJDK 17
Rust 1.79.0
```


## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgments
* [osmpbf](https://github.com/b-r-u/osmpbf) and [osm2gzip](https://github.com/b-r-u/osm2gzip) for reading PBF data
* [osm2orc](https://github.com/mojodna/osm2orc) for schema and processing ideas
