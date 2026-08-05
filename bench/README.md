# OSM transcode benchmark suite

Reproducible comparison of OSM PBF -> columnar transcode tools:
osm-pbf-parquet (this repo), DuckDB `ST_ReadOSM`, osm-parquetizer, osm2orc.

## Setup

1. `cp bench/config.env.example bench/config.env` and set tool paths.
   - DuckDB CLI: https://duckdb.org/docs/installation (spatial extension
     auto-installs on first run)
   - osm-parquetizer: clone https://github.com/adrianulbona/osm-parquetizer,
     `mvn package -DskipTests` (JDK 17)
   - osm2orc: clone https://github.com/mojodna/osm2orc, `./gradlew installDist`
2. Pick an input PBF (Geofabrik extract for quick runs, planet for the real
   numbers) and an output directory — ideally on a different physical disk
   than the input so read and write I/O don't contend.

## Run

```bash
bench/run.sh \
  --input /data/planet.osm.pbf \
  --output /scratch/osm-bench \
  --tools osm-pbf-parquet,duckdb,osm-parquetizer,osm2orc \
  --workers 8 --nice 10 --label planet-2026-02 \
  --validate
```

- `--workers` caps threads for tools that support it (osm-pbf-parquet,
  DuckDB). The Java tools use their own internal threading.
- `--nice` niceness for every tool run (default 10), so benchmarks coexist
  with other work on the machine.
- Results land in `bench/results/<label>/`: per-tool `time -v` logs,
  iostat samples for the input and output devices, `summary.tsv`
  (wall / user / sys / CPU% / max RSS / output bytes / file count /
  avg read and write MB/s), and `validation.txt` with `--validate`.

## Accuracy comparison (`--validate` or `bench/validate.py`)

Medium-weight, schema-aware aggregate validation: per element type it
compares row counts, id sums, tag-entry counts, way ref counts/sums,
relation member counts/ref sums/role lengths, and metadata sums (uid,
changeset) against a reference tool (default osm-pbf-parquet). Columns a
tool cannot produce are reported `N/A`, not failed — notably DuckDB's
`ST_ReadOSM` omits all element metadata (changeset, timestamp, uid, user,
version, visible).

It catches missing/duplicated/corrupted rows and values with high
probability, at a fraction of the cost of a row-by-row join. For a
full-pass equivalence check between two *same-schema* outputs, use an
order-independent row fingerprint instead:
`SELECT count(*), sum(hash(t)) FROM read_parquet('.../type=way/*.parquet') t`
per type on both sides.

## Interpreting results

- With input and output on separate disks, all four tools are CPU-bound on
  modern SSD/NVMe hardware; user+sys CPU seconds is the most transferable
  metric, wall time is what you experience at a given worker count.
- Output sizes are not directly comparable across tools: schemas differ
  (see above), as do container defaults (row-group size, compression).
