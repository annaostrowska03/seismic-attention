"""Pre-processing of raw Bluesky posts in Spark (deduplication, event time, quality flags).

Rules (decided for the project, see docs/bluesky.md):

- Deduplicate by ``uri`` (the same post matches several keywords). The copy from the earliest
  daily file is kept; ``n_copies`` tells how many raw rows the post had.
- ``created_ok``: createdAt lies inside the study period and is at most 5 minutes after
  indexedAt. Then ``t_event = createdAt`` (``time_source = "createdAt"``), otherwise
  ``t_event = indexedAt`` (``time_source = "indexedAt"``, ``bad_created_at = true``).
  The search API filters by indexedAt, so posts with a client-set createdAt from other years
  appear in the data; some real posts were also re-indexed months later.
- ``reindexed``: created_ok and indexedAt - createdAt > 1 day.
- ``is_clean``: created_ok and -5 <= indexedAt - createdAt <= 10 minutes (a live system sees a
  post only once it is indexed).
- ``unrelated_candidate``: the text contains an exclusion term (e.g. the game "Quake"). It is a
  flag only; no rows are removed at this stage.
- ``matched_keywords``: search keywords found in the lower-cased text (substring match, so
  "earthquake" also yields "quake"). The search service matches words in its own way, so the
  list can be empty for a post the search returned.

Raw rows are never deleted: the posts table keeps every unique post with its flags.
Must stay compatible with Python 3.8 (Spark containers) and use only built-in Spark functions
(no Python UDFs), so executors do not need this package.
"""
from __future__ import annotations

from typing import Dict, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAW_SCHEMA = T.StructType([
    T.StructField("uri", T.StringType()),
    T.StructField("createdAt", T.StringType()),
    T.StructField("indexedAt", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("langs", T.ArrayType(T.StringType())),
    T.StructField("did", T.StringType()),
    T.StructField("author_created", T.StringType()),
    T.StructField("reply", T.BooleanType()),
    T.StructField("likes", T.LongType()),
    T.StructField("reposts", T.LongType()),
    T.StructField("_corrupt_record", T.StringType()),
])

CREATED_TOLERANCE_MIN = 5       # createdAt may be at most this much after indexedAt
REINDEXED_AFTER_MIN = 24 * 60   # indexed more than 1 day after creation
CLEAN_MIN_LAG_MIN = -5
CLEAN_MAX_LAG_MIN = 10
OUT_OF_PERIOD = "out_of_period"

POST_COLUMNS = [
    "uri", "did", "text", "langs", "reply", "likes", "reposts",
    "created_at_raw", "indexed_at_raw", "author_created_raw",
    "created_ts", "indexed_ts", "t_event", "lag_min", "time_source",
    "bad_created_at", "reindexed", "in_period", "is_clean",
    "unrelated_candidate", "matched_keywords", "n_copies", "file_date",
    "event_date", "event_month",
]


def utc_ts(value: str):
    """Timestamp literal in UTC; a bare date ("2024-01-01") means midnight UTC."""
    if len(value) == 10:
        value += "T00:00:00Z"
    return F.to_timestamp(F.lit(value))


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read daily JSONL files (YYYY-MM-DD.jsonl) from a directory tree; other files are ignored."""
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl")
        .json(path)
        .withColumn("source_file", F.input_file_name())
        .withColumn("file_date",
                    F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})\.jsonl$", 1))
    )


def deduplicate(raw: DataFrame) -> DataFrame:
    """One row per uri (earliest daily file wins) with the number of raw copies."""
    valid = raw.where(F.col("uri").isNotNull())
    by_uri = Window.partitionBy("uri")
    order = by_uri.orderBy("file_date", "indexedAt", "createdAt")
    return (
        valid.withColumn("n_copies", F.count(F.lit(1)).over(by_uri))
        .withColumn("_rn", F.row_number().over(order))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def _contains_any(text_lower, terms: Sequence[str]):
    if not terms:
        return F.lit(False)
    cond = F.lit(False)
    for term in terms:
        cond = cond | text_lower.contains(term.lower())
    return F.coalesce(cond, F.lit(False))


def add_flags(
    posts: DataFrame,
    period_start: str,
    period_end: str,
    exclude_terms: Sequence[str] = (),
    keywords: Sequence[str] = (),
) -> DataFrame:
    """Add event time, quality flags and keyword columns. Period is [start, end) in UTC.

    The Spark session time zone must be UTC (createdAt values without an offset are read in it).
    """
    start = utc_ts(period_start)
    end = utc_ts(period_end)
    created = F.to_timestamp("createdAt")
    indexed = F.to_timestamp("indexedAt")
    text_lower = F.lower(F.col("text"))

    df = (
        posts.withColumn("created_ts", created)
        .withColumn("indexed_ts", indexed)
        .withColumn("lag_min",
                    (F.col("indexed_ts").cast("double") - F.col("created_ts").cast("double")) / 60)
    )
    created_ok = F.coalesce(
        (F.col("created_ts") >= start)
        & (F.col("created_ts") < end)
        & (F.col("created_ts")
           <= F.col("indexed_ts") + F.expr(f"INTERVAL {CREATED_TOLERANCE_MIN} MINUTES")),
        F.lit(False),
    )
    df = (
        df.withColumn("_created_ok", created_ok)
        .withColumn("t_event", F.when(F.col("_created_ok"), F.col("created_ts"))
                    .otherwise(F.col("indexed_ts")))
        .withColumn("time_source", F.when(F.col("_created_ok"), F.lit("createdAt"))
                    .otherwise(F.lit("indexedAt")))
        .withColumn("bad_created_at", ~F.col("_created_ok"))
        .withColumn("reindexed", F.col("_created_ok")
                    & F.coalesce(F.col("lag_min") > REINDEXED_AFTER_MIN, F.lit(False)))
        .withColumn("in_period", F.coalesce(
            (F.col("t_event") >= start) & (F.col("t_event") < end), F.lit(False)))
        .withColumn("is_clean", F.col("_created_ok")
                    & F.coalesce(F.col("lag_min").between(CLEAN_MIN_LAG_MIN, CLEAN_MAX_LAG_MIN),
                                 F.lit(False)))
        .withColumn("unrelated_candidate", _contains_any(text_lower, exclude_terms))
        .withColumn("matched_keywords", F.filter(
            F.array(*[F.when(text_lower.contains(k.lower()), F.lit(k)) for k in keywords])
            if keywords else F.array().cast("array<string>"),
            lambda x: x.isNotNull()))
        .withColumn("event_date", F.to_date("t_event"))
        # partition key: out-of-period posts share one partition instead of ~100 tiny ones
        .withColumn("event_month", F.when(F.col("in_period"), F.date_format("t_event", "yyyy-MM"))
                    .otherwise(F.lit(OUT_OF_PERIOD)))
        .withColumnRenamed("createdAt", "created_at_raw")
        .withColumnRenamed("indexedAt", "indexed_at_raw")
        .withColumnRenamed("author_created", "author_created_raw")
    )
    return df.select(*POST_COLUMNS)


def minute_counts(posts: DataFrame, period_start: str, period_end: str) -> DataFrame:
    """Dense per-minute counts of in-period posts (minutes without posts have zeros)."""
    spark = posts.sparkSession
    grid = spark.range(1).select(F.explode(F.sequence(
        utc_ts(period_start),
        utc_ts(period_end) - F.expr("INTERVAL 1 MINUTE"),
        F.expr("INTERVAL 1 MINUTE"),
    )).alias("minute"))
    counts = (
        posts.where("in_period")
        .groupBy(F.date_trunc("minute", "t_event").alias("minute"))
        .agg(
            F.count(F.lit(1)).alias("n_posts"),
            F.countDistinct("did").alias("n_authors"),
            F.sum(F.col("is_clean").cast("int")).alias("n_posts_clean"),
            F.countDistinct(F.when(F.col("is_clean"), F.col("did"))).alias("n_authors_clean"),
            F.sum(F.col("unrelated_candidate").cast("int")).alias("n_unrelated"),
        )
    )
    value_cols = ["n_posts", "n_authors", "n_posts_clean", "n_authors_clean", "n_unrelated"]
    out = grid.join(counts, "minute", "left")
    for c in value_cols:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("long"))
    return out.orderBy("minute")


def summarize(raw: DataFrame, posts: DataFrame) -> Dict[str, object]:
    """Control numbers for logs, docs and tests."""
    # Referencing a regular column too ("uri") keeps Spark from rejecting a query that would
    # touch only _corrupt_record of an uncached JSON source.
    raw_stats = raw.agg(
        F.count(F.lit(1)).alias("raw_rows"),
        F.count("uri").alias("raw_rows_with_uri"),
        F.sum(F.col("_corrupt_record").isNotNull().cast("int")).alias("corrupt_rows"),
        F.countDistinct("file_date").alias("raw_files"),
    ).first()
    p = posts.agg(
        F.count(F.lit(1)).alias("unique_posts"),
        F.sum(F.col("in_period").cast("int")).alias("in_period"),
        F.sum((F.col("in_period") & (F.col("time_source") == "createdAt")).cast("int"))
        .alias("in_period_time_source_createdAt"),
        F.sum((F.col("in_period") & (F.col("time_source") == "indexedAt")).cast("int"))
        .alias("in_period_time_source_indexedAt"),
        F.sum((F.col("in_period") & F.col("reindexed")).cast("int")).alias("in_period_reindexed"),
        F.sum((F.col("in_period") & F.col("is_clean")).cast("int")).alias("clean"),
        F.sum((F.col("in_period") & F.col("unrelated_candidate")).cast("int"))
        .alias("in_period_unrelated_candidate"),
        F.sum((F.col("created_ts").isNull()).cast("int")).alias("unparsable_created_at"),
        F.sum((F.col("indexed_ts").isNull()).cast("int")).alias("unparsable_indexed_at"),
        F.min(F.when(F.col("in_period"), F.col("t_event"))).alias("first_t_event"),
        F.max(F.when(F.col("in_period"), F.col("t_event"))).alias("last_t_event"),
    ).first()
    out = {k: (raw_stats[k] or 0)
           for k in ("raw_rows", "raw_rows_with_uri", "corrupt_rows", "raw_files")}
    for k, v in p.asDict().items():
        out[k] = v.isoformat() if hasattr(v, "isoformat") else (v or 0)
    out["out_of_period"] = out["unique_posts"] - out["in_period"]
    return out
