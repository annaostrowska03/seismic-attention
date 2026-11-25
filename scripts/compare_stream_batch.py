"""Compare the Bluesky minute counts of the streaming job with the batch table (test STR-Q-01).

Run inside the Spark container after both jobs have finished:

    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/compare_stream_batch.py

The streaming job (scripts/spark_bsky_streaming.py) writes partial counts per micro-batch, one folder
per batch id, so the same minute can appear in several folders: the stream side is summed per minute
first. The batch table /curated/bsky/minute (Hive bsky_minute, scripts/spark_bsky_preprocess.py) has a
row for every minute of the period, also minutes without posts. A full outer join on the minute with
missing counts set to 0 compares both sides.

Prints one "COMPARE {...}" line and exits with code 1 if any minute differs.
"""
import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

STREAM_PATH = "hdfs://namenode:9000/curated/bsky/stream_minute_parts_real_v1"
BATCH_PATH = "hdfs://namenode:9000/curated/bsky/minute"
COUNT_COLUMNS = ["stream_posts", "stream_clean", "batch_posts", "batch_clean"]


def compare(stream_parts, batch_minutes):
    """Totals of both sides and the number of minutes in which they differ."""
    stream = stream_parts.groupBy("minute").agg(
        F.sum("n_posts").alias("stream_posts"),
        F.sum("n_clean").alias("stream_clean"),
    )
    batch = batch_minutes.select(
        "minute",
        F.col("n_posts").alias("batch_posts"),
        F.col("n_posts_clean").alias("batch_clean"),
    )
    joined = batch.join(stream, "minute", "full_outer").fillna(0, subset=COUNT_COLUMNS)
    differs = ((F.col("stream_posts") != F.col("batch_posts"))
               | (F.col("stream_clean") != F.col("batch_clean")))
    row = joined.agg(
        F.sum(((F.col("stream_posts") > 0) | (F.col("batch_posts") > 0)).cast("int"))
        .alias("minutes_with_posts"),
        F.sum("stream_posts").alias("stream_posts"),
        F.sum("batch_posts").alias("batch_posts"),
        F.sum("stream_clean").alias("stream_clean"),
        F.sum("batch_clean").alias("batch_clean"),
        F.sum(differs.cast("int")).alias("minutes_different"),
    ).first()
    return {k: int(v or 0) for k, v in row.asDict().items()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stream", default=STREAM_PATH, help="output folder of the streaming job")
    ap.add_argument("--batch", default=BATCH_PATH, help="per-minute table of the batch job")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("compare-stream-batch")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    result = compare(spark.read.parquet(args.stream), spark.read.parquet(args.batch))
    spark.stop()

    print("COMPARE " + json.dumps(result, sort_keys=True))
    checks = [
        ("total posts equal", result["stream_posts"] == result["batch_posts"]),
        ("total clean posts equal", result["stream_clean"] == result["batch_clean"]),
        ("no minute differs", result["minutes_different"] == 0),
    ]
    for name, ok in checks:
        print(f"[{'OK' if ok else 'FAIL':>4}] {name}")
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
