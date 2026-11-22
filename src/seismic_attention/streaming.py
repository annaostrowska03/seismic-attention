
from pyspark.sql import functions as F

from seismic_attention.config import PERIOD_START, PERIOD_END
from seismic_attention.ingest.keywords import load_keywords
from seismic_attention.preprocess.bluesky import RAW_SCHEMA, add_flags


def unique_posts(messages):
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
    return (
        batch
        .where(F.col("in_period") & F.col("t_event").isNotNull())
        .groupBy(F.date_trunc("minute", "t_event").alias("minute"))
        .agg(
            F.count("*").alias("n_posts"),
            F.sum(F.col("is_clean").cast("int")).alias("n_clean"),
        )
    )
