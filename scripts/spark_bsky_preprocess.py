"""Spark batch job: raw Bluesky JSONL -> curated Parquet (posts, clean posts, minute counts).

Run inside the Spark container (paths below are container paths):

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        /opt/app/scripts/spark_bsky_preprocess.py

    # development on the local copy instead of HDFS (separate summary file, so that
    # scripts/hive_bsky.py verify still compares Hive with the HDFS run):
    ... spark_bsky_preprocess.py --input file:///opt/app/data/raw/bsky \
        --output file:///opt/app/data/processed/bsky \
        --summary-file /opt/app/data/processed/bsky_summary_local.json

Outputs (under --output):
    posts/        every unique post with flags, partitioned by event_month
    posts_clean/  in-period posts with is_clean = true, partitioned by event_month
    minute/       dense per-minute counts for the period
Control numbers are printed as one "SUMMARY {...}" line and written to --summary-file.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))  # /opt/app/src in the container

from pyspark.sql import SparkSession  # noqa: E402

from seismic_attention.ingest.keywords import load_keywords  # noqa: E402
from seismic_attention.preprocess import bluesky as bp  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default="hdfs://namenode:9000/raw/bsky")
    ap.add_argument("--output", default="hdfs://namenode:9000/curated/bsky")
    ap.add_argument("--start", default="2024-01-01", help="period start, UTC, inclusive")
    ap.add_argument("--end", default="2025-01-01", help="period end, UTC, exclusive")
    ap.add_argument("--summary-file", default=str(ROOT / "data" / "processed" / "bsky_summary.json"))
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("bsky-preprocess")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    cfg = load_keywords()

    raw = bp.read_raw(spark, args.input).cache()
    posts = bp.add_flags(bp.deduplicate(raw), args.start, args.end,
                         exclude_terms=cfg.exclude_terms, keywords=cfg.search_keywords).cache()

    out = args.output.rstrip("/")
    posts.write.mode("overwrite").partitionBy("event_month").parquet(f"{out}/posts")
    (posts.where("in_period AND is_clean")
     .write.mode("overwrite").partitionBy("event_month").parquet(f"{out}/posts_clean"))
    (bp.minute_counts(posts, args.start, args.end)
     .coalesce(1).write.mode("overwrite").parquet(f"{out}/minute"))

    summary = bp.summarize(raw, posts)
    summary.update({"input": args.input, "output": out, "period": [args.start, args.end]})
    print("SUMMARY " + json.dumps(summary, sort_keys=True))
    Path(args.summary_file).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_file).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n",
                                       encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
