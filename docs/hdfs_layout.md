# HDFS layout and Hive tables

One place to see where each source lands in HDFS and which Hive table (database `seismic`) reads it.
All Hive tables are **external**: dropping a table never deletes its files.

## Zones

| Zone | Written by | Format | Purpose |
|---|---|---|---|
| `/raw/<source>/date=YYYY-MM-DD/` | NiFi | as received (JSON lines) | raw zone: unchanged input, replay source |
| `/curated/<source>/...` | Spark jobs | Parquet | cleaned data, features |
| `/history/<table>/` | `scripts/load_history.py` | Parquet | one-off backfill of USGS and GDELT, 2024-01-01 – 2026-11-20, for labels, training and the exploratory analysis (labels use the study period of `src/seismic_attention/config.py`) |
| `/checkpoints` | Spark Structured Streaming | – | streaming state |

**Team convention for the raw zone:** one folder per UTC day, `date=YYYY-MM-DD`. Hive reads the folder name as the
partition column `date`, so `WHERE date = '2025-03-28'` reads one folder only. New folders become visible in Hive
after `MSCK REPAIR TABLE <table>` (the `create` commands of the scripts below run it).

## Sources

| Source | HDFS | Hive tables | Created by |
|---|---|---|---|
| Bluesky | `/raw/bsky/date=.../<day>.jsonl` | `bsky_raw` | `hive/bsky.sql` (`scripts/hive_bsky.py create`) |
| | `/curated/bsky/{posts,posts_clean,minute}` | `bsky_posts`, `bsky_posts_clean`, `bsky_minute` | same |
| GDELT | `/raw/gdelt/date=.../<stamp>.jsonl` (earthquake articles) | `gdelt_raw` | `hive/gdelt.sql` (`scripts/hive_gdelt.py create`) |
| | `/raw/gdelt_files/date=.../<stamp>.jsonl` (file log: ok / missing / broken) | `gdelt_files_raw` | same |
| | `/history/gdelt_history`, `/history/gdelt_files` | `gdelt_history`, `gdelt_files` | `scripts/load_history.py` |
| USGS | `/raw/usgs/date=.../usgs-<ingest time>-<id>-<updated>.json` (one event version per file, NiFi flow `nifi/flows/usgs.json`) | `usgs_raw` (nested, as received), view `usgs_raw_flat` (columns of `usgs_history`) | `hive/usgs.sql` (`scripts/hive_usgs.py create`) |
| | `/history/usgs_history` | `usgs_history` | `scripts/load_history.py` |
| Labels | `/curated/coverage_labels` (Parquet, one row per earthquake) | `coverage_labels` | `scripts/spark_coverage_labels.py` (rule: `src/seismic_attention/labels.py`) |
| Bluesky stream | `/curated/bsky/stream_minute_parts_real_v1/batch_id=NNNNNNNN/` (Parquet, partial minute counts per micro-batch) | – (read by `scripts/compare_stream_batch.py`) | `scripts/spark_bsky_streaming.py` |
| | `/checkpoints/bsky-minute-ms2-real-v1/` (Kafka offsets, deduplication state) | – | same |

## Outside HDFS

- **Kafka topics** `usgs`, `gdelt`, `bsky` (3 partitions each): one message per record, written by NiFi;
  `usgs` is keyed by the event id. NiFi publishes in transactions, so consumers use `read_committed`.
- **PostgreSQL** holds only the Hive metastore (table definitions), no project data.
- **Planned (MS3):** the serving layer in Apache Cassandra, with batch views written by the Spark batch
  jobs and real-time views written by Spark Structured Streaming; see [architecture.md](architecture.md).

`<stamp>` is the time of the GDELT file, `yyyyMMddHHmmss` UTC (one file every 15 minutes).

## Notes

- `gdelt_raw` has no column `date` from the JSON lines: Hive does not allow a column with the name of the partition
  column. The file time is the first 14 characters of `gkg_id`.
- `date`, `time` and `file` are reserved words in Hive and need backticks.
- `scripts/hive_gdelt.py verify` checks that the articles in `gdelt_raw` agree with the file log `gdelt_files_raw`.
- USGS: the `date` folder is the UTC day of ingest, not of the earthquake. `usgs_raw` keeps every new version of an
  event (same `id`, new `updated`); `geometry.coordinates` is `[longitude, latitude, depth_km]`.
  `scripts/hive_usgs.py verify` checks unique versions, coordinates and partition dates.
