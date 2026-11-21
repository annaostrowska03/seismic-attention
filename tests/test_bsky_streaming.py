
import json
import os
import shutil

import pytest

if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    pytest.skip("Java is required", allow_module_level=True)

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from seismic_attention.streaming import unique_posts, minute_counts


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-bsky-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.jars.packages", "")
        .getOrCreate()
    )

    yield session
    session.stop()


def post(uri, minute, second=5):
    time = f"2025-03-01T10:{minute:02d}:{second:02d}Z"
    return {
        "uri": uri,
        "createdAt": time,
        "indexedAt": time,
        "text": "earthquake",
        "did": uri,
    }


def test_dedup_survives_restart(spark, tmp_path):
    source = tmp_path / "input"
    output = tmp_path / "output"
    checkpoint = tmp_path / "checkpoint"
    source.mkdir()

    def write_posts(name, posts):
        (source / name).write_text(
            "\n".join(json.dumps(p) for p in posts) + "\n",
            encoding="utf-8",
        )

    def process_batch(batch, batch_id):
        result = minute_counts(batch)
        result.write.mode("overwrite").parquet(
            str(output / f"batch_id={batch_id:08d}")
        )

    def run_stream():
        messages = spark.readStream.format("text").load(str(source))

        query = (
            unique_posts(messages)
            .writeStream
            .foreachBatch(process_batch)
            .outputMode("append")
            .option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start()
        )

        assert query.awaitTermination(120)

    write_posts(
        "batch1.txt",
        [
            post("at://test/1", 0),
            post("at://test/2", 0, 30),
            post("at://test/3", 1),
            post("at://test/1", 0),
        ],
    )

    run_stream()

    write_posts(
        "batch2.txt",
        [
            post("at://test/2", 0, 30),
            post("at://test/4", 2),
        ],
    )

    run_stream()

    result = (
        spark.read.parquet(str(output))
        .groupBy("minute")
        .agg(
            F.sum("n_posts").alias("n_posts"),
            F.sum("n_clean").alias("n_clean"),
        )
        .collect()
    )

    actual = {
        row["minute"].strftime("%Y-%m-%d %H:%M"):
            (row["n_posts"], row["n_clean"])
        for row in result
    }

    assert actual == {
        "2025-03-01 10:00": (2, 2),
        "2025-03-01 10:01": (1, 1),
        "2025-03-01 10:02": (1, 1),
    }
