"""Sedona single-node OSM PBF -> parquet transcode job.

Usage: sedona_job.py <input.osm.pbf> <outdir> <workers>
Env: SPARK_DRIVER_MEMORY (default 24g), SEDONA_PACKAGE (maven coordinate)
"""
import os
import sys

from sedona.spark import SedonaContext

inp, outdir, workers = sys.argv[1], sys.argv[2], int(sys.argv[3])
driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "24g")
package = os.environ.get(
    "SEDONA_PACKAGE", "org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.0"
)

config = (
    SedonaContext.builder()
    .master(f"local[{workers}]")
    .config("spark.driver.memory", driver_mem)
    .config("spark.jars.packages", package)
    .config("spark.sql.parquet.compression.codec", "zstd")
    .config("spark.io.compression.zstd.level", "3")
    .config("parquet.compression.codec.zstd.level", "3")
    .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", "/tmp"))
    .getOrCreate()
)
sedona = SedonaContext.create(config)

df = sedona.read.format("osmpbf").load(inp)
df.write.partitionBy("kind").parquet(outdir, mode="overwrite")
