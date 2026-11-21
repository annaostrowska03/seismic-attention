# HDFS layout and Hive tables

One place to see where each source lands in HDFS and which Hive table (database `seismic`) reads it.
All Hive tables are **external**: dropping a table never deletes its files.

## Zones

| Zone | Written by | Format | Purpose |
|---|---|---|---|
| `/raw/<source>/date=YYYY-MM-DD/` | NiFi | as received (JSON lines) | raw zone: unchanged input, replay source |
| `/curated/<source>/...` | Spark jobs | Parquet | cleaned data, features |
| `/history/<table>/` | `scripts/load_history.py` | Parquet | one-off backfill 2023-2025 (USGS, GDELT) for labels and training |
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
| USGS | `/raw/usgs/...` (NiFi flow `nifi/flows/usgs.json`) | – (to be added once the flow writes `date=` folders) | – |
| | `/history/usgs_history` | `usgs_history` | `scripts/load_history.py` |

`<stamp>` is the time of the GDELT file, `yyyyMMddHHmmss` UTC (one file every 15 minutes).

## Notes

- `gdelt_raw` has no column `date` from the JSON lines: Hive does not allow a column with the name of the partition
  column. The file time is the first 14 characters of `gkg_id`.
- `date`, `time` and `file` are reserved words in Hive and need backticks.
- `scripts/hive_gdelt.py verify` checks that the articles in `gdelt_raw` agree with the file log `gdelt_files_raw`.
