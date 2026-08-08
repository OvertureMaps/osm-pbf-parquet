"""Sedona single-node OSM PBF -> parquet transcode job.

Usage: sedona_job.py <input.osm.pbf> <outdir> <workers>
<outdir> may be a local path or an s3a:// URL. An s3a:// output adds the
hadoop-aws package and points fs.s3a at the bench MinIO endpoint
(defaults below, env-overridable); local output is unchanged.
Env: SPARK_DRIVER_MEMORY (default 24g), SEDONA_PACKAGE (maven coordinate),
HADOOP_AWS_PACKAGE, BENCH_S3_ENDPOINT, BENCH_S3_ACCESS_KEY,
BENCH_S3_SECRET_KEY, BENCH_S3_REGION
"""
import os
import sys

from sedona.spark import SedonaContext

inp, outdir, workers = sys.argv[1], sys.argv[2], int(sys.argv[3])
driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "24g")
package = os.environ.get(
    "SEDONA_PACKAGE", "org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.0"
)
packages = package
s3a_confs = {}
if outdir.startswith("s3a://"):
    # pyspark 4.0.1 bundles Hadoop 3.4.x client jars; hadoop-aws must match.
    packages += "," + os.environ.get(
        "HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.4.1"
    )
    s3a_confs = {
        "spark.hadoop.fs.s3a.endpoint": os.environ.get(
            "BENCH_S3_ENDPOINT", "http://127.0.0.1:9102"
        ),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": os.environ.get(
            "BENCH_S3_ACCESS_KEY", "bench"
        ),
        "spark.hadoop.fs.s3a.secret.key": os.environ.get(
            "BENCH_S3_SECRET_KEY", "benchsecret123"
        ),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        # AWS SDK v2 (Hadoop 3.4.x) wants an explicit region with a
        # custom endpoint.
        "spark.hadoop.fs.s3a.endpoint.region": os.environ.get(
            "BENCH_S3_REGION", "us-east-1"
        ),
    }

builder = (
    SedonaContext.builder()
    .master(f"local[{workers}]")
    .config("spark.driver.memory", driver_mem)
    .config("spark.jars.packages", packages)
    .config("spark.sql.parquet.compression.codec", "zstd")
    .config("spark.io.compression.zstd.level", "3")
    .config("parquet.compression.codec.zstd.level", "3")
    .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", "/tmp"))
)
for key, value in s3a_confs.items():
    builder = builder.config(key, value)
config = builder.getOrCreate()
sedona = SedonaContext.create(config)

df = sedona.read.format("osmpbf").load(inp)
df.write.partitionBy("kind").parquet(outdir, mode="overwrite")
