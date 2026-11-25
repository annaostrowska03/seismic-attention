"""Spark Structured Streaming job: Kafka topic "bsky" -> per-minute counts of unique posts in HDFS.

Run inside the Spark container (settings of the full 2024-2025 run, see docs/streaming.md):

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \\
        --conf spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage=true \\
        --conf spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB=256 \\
        --conf spark.sql.shuffle.partitions=8 \\
        /opt/app/scripts/spark_bsky_streaming.py

Reads the topic from the earliest offset (read_committed, 20 000 offsets per micro-batch), processes
what is available and stops (trigger availableNow). Every micro-batch writes its partial counts to
OUTPUT_PATH/batch_id=NNNNNNNN; the checkpoint keeps the Kafka offsets and the deduplication state, so a
restart with the same checkpoint does not count a post twice. For a new full replay use a new output
path and a new checkpoint. Check against the batch job: scripts/compare_stream_batch.py.
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from seismic_attention.streaming import unique_posts, minute_counts


OUTPUT_PATH = "hdfs://namenode:9000/curated/bsky/stream_minute_parts_real_v1"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/bsky-minute-ms2-real-v1"


def process_batch(batch, batch_id):
    """foreachBatch sink: minute counts of one micro-batch to its own folder (rerun overwrites it)."""
    minutes = minute_counts(batch)

    print(f"\nBatch {batch_id}")

    output = f"{OUTPUT_PATH}/batch_id={batch_id:08d}"
    minutes.write.mode("overwrite").parquet(output)


def main():
    spark = (
        SparkSession.builder
        .appName("bsky-minute-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "bsky")
        .option("startingOffsets", "earliest")
        .option("kafka.isolation.level", "read_committed")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    query = (
        unique_posts(kafka)
        .writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
