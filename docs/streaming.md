# Bluesky Spark Structured Streaming

**Status:** Implemented and validated on the complete 2024–2025 Bluesky dataset (2026-11-22)

## 1. Purpose

This module processes historical Bluesky messages replayed through Kafka using **Apache Spark Structured Streaming**. Its purpose is to produce minute-level counts of unique posts, including a separate count of posts satisfying the project's `is_clean` quality rule.

It complements the batch preprocessing pipeline described in [bluesky.md](bluesky.md). Both pipelines apply the same event-time and post-quality rules, which makes it possible to validate the streaming output against the batch reference.

## 2. Data flow

```text
Bluesky JSONL files (2024–2025)
         |
         v
      Apache NiFi
         |
         v
   Kafka topic: bsky
         |
         v
 Spark Structured Streaming
   - parse JSON messages
   - derive event time and quality flags
   - deduplicate by post URI (stateful)
   - aggregate unique posts by event minute
         |
         v
 HDFS /curated/bsky/stream_minute_parts_real_v1/
   - Parquet files, one directory per microbatch

 HDFS /checkpoints/bsky-minute-ms2-real-v1/
   - Kafka offsets, query progress and deduplication state
```

The source is Kafka, not the HDFS raw zone. NiFi sends one Kafka message per raw JSONL line; the topic can therefore contain multiple copies of the same post. The study period is **2024-01-01 to 2026-01-01 (exclusive), UTC**.

## 3. Implementation

| File | Responsibility |
|---|---|
| [`src/seismic_attention/streaming.py`](../src/seismic_attention/streaming.py) | Parse Kafka JSON, apply shared Bluesky preprocessing rules, deduplicate posts and calculate minute counts. |
| [`scripts/spark_bsky_streaming.py`](../scripts/spark_bsky_streaming.py) | Configure Spark and Kafka, manage the streaming query, write Parquet output and maintain the checkpoint. |
| [`tests/test_bsky_streaming.py`](../tests/test_bsky_streaming.py) | Test deduplication, minute counts and recovery across a streaming-query restart. |
| [`src/seismic_attention/preprocess/bluesky.py`](../src/seismic_attention/preprocess/bluesky.py) | Shared raw schema and event-time/quality logic (`RAW_SCHEMA`, `add_flags`). |

### 3.1 Parsing and deduplication

`unique_posts()` parses each Kafka `value` as JSON using the project's `RAW_SCHEMA`, filters out messages with a missing `uri`, and applies the same time and quality flags used by batch processing. It then uses `dropDuplicates(["uri"])` to retain one occurrence of each post across microbatches.

The operation is **stateful**: Spark maintains the set of previously seen post URIs in its state store. Since no event-time watermark is configured, previously observed URIs are not automatically expired.

Unlike batch preprocessing, streaming does not retain the earliest copy by `file_date` or calculate the actual number of copies per URI. Its intermediate `file_date` is null and `n_copies` is set to 1. These fields are not used by the two output metrics.

### 3.2 Minute-level aggregation

`minute_counts()` keeps posts whose `t_event` falls within the study period and groups them by `date_trunc("minute", "t_event")`.

| Output field | Meaning |
|---|---|
| `minute` | Start of the event-time minute (UTC). |
| `n_posts` | Number of unique, in-period posts assigned to that minute. |
| `n_clean` | Number of these posts with `is_clean = true`. |

The counts are **partial results per microbatch**. A minute may appear in more than one output directory; consumers must sum both columns by `minute` to obtain the final counts. Minutes with no posts are not emitted by the streaming job. The batch `bsky_minute` table, in contrast, includes a dense minute grid with zero counts.

## 4. Execution

### Prerequisites

1. Start the Docker stack (see [SETUP.md](SETUP.md)).
2. Load the 2024–2025 Bluesky JSONL files into the Kafka topic `bsky` through the NiFi ingestion flow (see [bluesky.md](bluesky.md)).
3. Keep Kafka, HDFS and the Spark master/worker running. The batch preprocessing job is also needed if batch–stream validation is to be performed.

The full-dataset run used the following command from the repository root:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
  --conf spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage=true \
  --conf spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB=256 \
  --conf spark.sql.shuffle.partitions=8 \
  /opt/app/scripts/spark_bsky_streaming.py
```

Relevant settings in `spark_bsky_streaming.py`:

| Setting | Value | Reason |
|---|---|---|
| Kafka bootstrap server | `kafka:9092` | Kafka address inside Docker. |
| Kafka topic | `bsky` | Source of replayed Bluesky messages. |
| `startingOffsets` | `earliest` | Read historical records on a fresh query/checkpoint. |
| `kafka.isolation.level` | `read_committed` | Ignore uncommitted Kafka transactional records. |
| `maxOffsetsPerTrigger` | `20000` | Limit the number of Kafka offsets processed in a microbatch. |
| Trigger | `availableNow=True` | Process the data available at query start, then terminate. |
| State store | RocksDB | Keep the large deduplication state outside the default JVM heap-based state store. |
| Spark shuffle partitions | `8` | Configuration used in the full-dataset run. |
| Spark session timezone | `UTC` | Interpret event timestamps consistently with batch processing. |

The initial offsets setting applies only when there is no existing checkpoint. With the same checkpoint, Spark resumes from its recorded progress rather than replaying the topic from the beginning.

### HDFS output and checkpoint

```text
/curated/bsky/stream_minute_parts_real_v1/batch_id=00000000/
/curated/bsky/stream_minute_parts_real_v1/batch_id=00000001/
...
/checkpoints/bsky-minute-ms2-real-v1/
```

Each microbatch writes its partial minute counts to a deterministic `batch_id` directory in **Parquet** format. The checkpoint tracks input progress and state, allowing a subsequent query run to resume. To perform an independent replay from the earliest Kafka offsets, use a **new, separate output path and checkpoint path**; do not combine the new replay with the previous results.

## 5. Testing and validation

### 5.1 Automated streaming test

The test `test_dedup_survives_restart` in `tests/test_bsky_streaming.py` writes input records in two stages. The first run includes a repeated URI; after stopping the query, the second stage repeats an earlier URI and adds a new post. The query is restarted with the **same checkpoint**. The expected per-minute counts are asserted, verifying that previously processed posts are not counted again.

Run this test in the Spark container:

```bash
python scripts/run_spark_tests.py -k dedup_survives_restart
```

To run the complete automated suite:

```bash
python scripts/run_spark_tests.py
```

The full suite (all modules, 82 tests) passes: see [testing.md](testing.md).

### 5.2 Full-dataset integration test

NiFi ingestion was verified before the streaming run: **731 daily files** and **1,973,106 Kafka messages**, with zero invalid JSON messages. The batch reference contained **1,371,078 unique posts**, including **1,370,530 in-period posts** and **1,324,627 clean posts**.

The streaming job processed the historical Kafka input in **99 microbatches (IDs 0–98)** and wrote its results to HDFS. The reported comparison was:

| Metric | Batch reference | Streaming result |
|---|---:|---:|
| Total in-period posts (`n_posts`) | 1,370,530 | 1,370,530 |
| Total clean posts (`n_clean`) | 1,324,627 | 1,324,627 |
| Minutes with different counts | — | **0** |

Both global totals matched exactly. A full outer join on `minute`, with missing counts treated as zero, also found **zero minute-level differences** for both metrics over the complete study period.

### 5.3 Reproduce the batch–stream comparison

After completing the batch and streaming jobs, run the comparison script (test STR-Q-01):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/compare_stream_batch.py
```

It sums the per-micro-batch parts of `/curated/bsky/stream_minute_parts_real_v1` by `minute`, joins them
with the batch table `/curated/bsky/minute` (full outer join, missing counts = 0) and prints one
`COMPARE {...}` line; the exit code is 1 if any minute differs.

**Observed output (2026-11-25):**

```text
COMPARE {"batch_clean": 1324627, "batch_posts": 1370530, "minutes_different": 0, "minutes_with_posts": 539714, "stream_clean": 1324627, "stream_posts": 1370530}
[  OK] total posts equal
[  OK] total clean posts equal
[  OK] no minute differs
```

Log: [test_evidence/team/05_stream_vs_batch.txt](test_evidence/team/05_stream_vs_batch.txt).

## 6. Limitations and next steps

- **Unbounded deduplication state.** Without a watermark or another retention policy, the number of stored URIs keeps growing as new posts arrive. RocksDB reduces pressure on the JVM heap but does not bound the logical state size.
- **Historical catch-up, not an always-on demo by default.** The `availableNow` trigger is intended for finite replay or catch-up runs. A continuously running feed would use a different trigger strategy.
- **Sparse, partial output.** Output rows are microbatch-level increments; downstream processing must aggregate them by minute and optionally generate the missing zero-count minutes.
- **Not the complete batch schema.** The streaming output includes post and clean-post counts, but not the batch table's distinct-author or unrelated-post metrics.
- **Deduplication policy.** Streaming keeps the first observed version of a URI, while batch preprocessing chooses a copy using daily-file ordering. Their minute-level counts matched on the tested dataset, but this does not establish equivalence for all possible input orders or updated post versions.
- **Checkpoint and output consistency.** The Parquet `foreachBatch` sink is not a transactional sink spanning Kafka offsets, Spark state and HDFS. Deterministic batch paths help with retries, but output files and the checkpoint must be managed together; avoid deleting either one independently.

**Next step (MS3).** In the planned simulated run ([architecture.md](architecture.md), section 5) the job
runs continuously (processing-time trigger instead of `availableNow`), uses an event-time watermark to bound
the deduplication state and writes the minute counts to the real-time view in Apache Cassandra.

For details of the source data, quality rules, HDFS/Hive tables and NiFi ingestion, see [Bluesky data source](bluesky.md).
