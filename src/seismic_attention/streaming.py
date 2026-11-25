"""Bluesky speed layer: Spark Structured Streaming on the Kafka topic "bsky".

The same event-time and quality rules as the batch job (seismic_attention.preprocess.bluesky.add_flags)
are applied to every message, so the streaming counts can be checked against the batch table
(scripts/compare_stream_batch.py). Used by scripts/spark_bsky_streaming.py; see docs/streaming.md.
"""
from pyspark.sql import functions as F

from seismic_attention.config import PERIOD_START, PERIOD_END
from seismic_attention.ingest.keywords import load_keywords
from seismic_attention.preprocess.bluesky import RAW_SCHEMA, add_flags


def unique_posts(messages):
    """Kafka messages (column "value" = one raw JSON line) -> unique posts with quality flags.

    Messages without a uri are dropped. Deduplication by uri is stateful: Spark keeps the seen uris
    in its state store across micro-batches (no watermark yet, so the state grows with the posts).
    file_date and n_copies only exist in the batch job; here they are null and 1.
    """
    posts = (
        messages
        .select(
            F.from_json(
                F.col("value").cast("string"),
                RAW_SCHEMA,
            ).alias("post")
        )
        .select("post.*")
        .where(F.col("uri").isNotNull())
        .withColumn("file_date", F.lit(None).cast("string"))
        .withColumn("n_copies", F.lit(1).cast("long"))
    )

    keywords = load_keywords()

    posts = add_flags(
        posts,
        PERIOD_START,
        PERIOD_END,
        exclude_terms=keywords.exclude_terms,
        keywords=keywords.search_keywords,
    )

    return posts.dropDuplicates(["uri"])


def minute_counts(batch):
    """Posts per event-time minute of one micro-batch: n_posts (unique, in period) and n_clean.

    The result is partial: a minute can get more posts in a later micro-batch, so readers sum the
    parts by minute. Minutes without posts are not written (the batch table bsky_minute has them).
    """
    return (
        batch
        .where(F.col("in_period") & F.col("t_event").isNotNull())
        .groupBy(F.date_trunc("minute", "t_event").alias("minute"))
        .agg(
            F.count("*").alias("n_posts"),
            F.sum(F.col("is_clean").cast("int")).alias("n_clean"),
        )
    )
