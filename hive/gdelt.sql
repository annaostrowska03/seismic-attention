-- External Hive tables for the GDELT source (database "seismic"), written by the NiFi flow
-- scripts/nifi_gdelt_flow.py. Run: python scripts/hive_gdelt.py create
-- Dropping an EXTERNAL table never deletes its files in HDFS, so this script is safe to re-run.
-- Not the same as seismic.gdelt_history / seismic.gdelt_files (history, scripts/load_history.py).

CREATE DATABASE IF NOT EXISTS seismic;
USE seismic;

-- Earthquake articles, one JSON line per article: /raw/gdelt/date=YYYY-MM-DD/<stamp>.jsonl
-- The JSON lines also have a field "date" (= the file time). Hive does not allow a column with the
-- name of the partition column, so it is not a column here: the file time is the first 14
-- characters of gkg_id, e.g. to_timestamp: from_unixtime(unix_timestamp(substr(gkg_id, 1, 14), 'yyyyMMddHHmmss')).
DROP TABLE IF EXISTS gdelt_raw;
CREATE EXTERNAL TABLE gdelt_raw (
  gkg_id     STRING COMMENT 'GDELT record id, starts with the file time yyyyMMddHHmmss (UTC)',
  source     STRING COMMENT 'domain of the news site',
  url        STRING,
  themes     STRING COMMENT 'GDELT themes separated by ;',
  locations  STRING COMMENT 'raw GKG locations: places separated by ;, fields by # (type#name#country#adm1#lat#lon#id)'
)
COMMENT 'Raw GDELT GKG rows with an EARTHQUAKE theme (same rows and fields as scripts/backfill_gdelt.py)'
PARTITIONED BY (`date` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/gdelt';
MSCK REPAIR TABLE gdelt_raw;

-- File log, one line per GDELT file: /raw/gdelt_files/date=YYYY-MM-DD/<stamp>.jsonl
-- Same statuses as data/raw/gdelt_history/files/*.csv: ok | missing | broken.
-- A file the flow never asked for (NiFi or the laptop off) has no row: compare with 96 files per day.
DROP TABLE IF EXISTS gdelt_files_raw;
CREATE EXTERNAL TABLE gdelt_files_raw (
  `file`     STRING COMMENT 'file time yyyyMMddHHmmss (UTC)',
  status     STRING COMMENT 'ok | missing | broken',
  articles   INT    COMMENT 'earthquake articles in the file (0 unless status = ok)'
)
COMMENT 'Log of the GDELT files taken by the NiFi flow'
PARTITIONED BY (`date` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/gdelt_files';
MSCK REPAIR TABLE gdelt_files_raw;
