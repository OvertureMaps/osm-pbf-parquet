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
1. [Install rust](https://www.rust-lang.org/tools/install) and [just](https://github.com/casey/just)
2. Clone repo `git clone https://github.com/OvertureMaps/osm-pbf-parquet.git`
3. Make changes
4. Run against PBF with `cargo run -- --input your.osm.pbf` ([Geofabrik regional PBF extracts here](https://download.geofabrik.de/))
5. Run `just --list` to see available dev commands (`just test`, `just clippy`, `just ci-test`, etc.)


## Benchmarks
osm-pbf-parquet prioritizes transcode speed over file size, file count or preserving ordering.

On the 2026-08-03 OSM planet PBF (94GB, 12.0B elements) with zstd:3 and 8 worker threads (Core Ultra 7 265K, 64GB, input on NVMe, output on a separate SATA SSD), osm-pbf-parquet transcodes the full planet in **11m45s** (4,602s CPU, 193GB output).

Comparison against other PBF→parquet paths on the same machine and planet file,
each pinned to the same 8 performance cores and writing zstd level-3 parquet,
with outputs verified equivalent via aggregate checksums. CPU time and peak
memory are measured from a per-tool cgroup, so they include all child
processes:

| | Wall time | CPU time | Peak memory | Output size | Files |
| - | - | - | - | - | - |
| **osm-pbf-parquet** | 11m45s | 4,602s | 8.4GiB | 193GB | 1,682 |
| [DuckDB](https://duckdb.org) 1.5 spatial `ST_ReadOSM` | 15m40s | 7,018s | 6.3GiB | 195GB¹ | 3 |
| [Apache Sedona](https://sedona.apache.org) 1.9 (single-node Spark, pyspark 4.0.1) | 61m16s | 28,625s | 25.5GiB | 227GB | 1,410 |

¹ `ST_ReadOSM` emits no metadata columns (changeset/timestamp/uid/user/version/visible), so it writes less data per row.


## License
Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgments
* [osmpbf](https://github.com/b-r-u/osmpbf) and [osm2gzip](https://github.com/b-r-u/osm2gzip) for reading PBF data
* [osm2orc](https://github.com/mojodna/osm2orc) for schema and processing ideas
