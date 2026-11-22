
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from seismic_attention.streaming import unique_posts, minute_counts


OUTPUT_PATH = "hdfs://namenode:9000/curated/bsky/stream_minute_parts_real_v1"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/bsky-minute-ms2-real-v1"


def process_batch(batch, batch_id):
    minutes = minute_counts(batch)

    print(f"\nBatch {batch_id}")
    #minutes.orderBy("minute").show(truncate=False)

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
