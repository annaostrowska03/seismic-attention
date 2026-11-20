-- External Hive tables for the Bluesky source (database "seismic").
-- Run: python scripts/hive_bsky.py create
-- Dropping an EXTERNAL table never deletes its files in HDFS, so this script is safe to re-run.

CREATE DATABASE IF NOT EXISTS seismic;
USE seismic;

-- Raw zone written by NiFi: one JSONL file per day, /raw/bsky/date=YYYY-MM-DD/YYYY-MM-DD.jsonl
DROP TABLE IF EXISTS bsky_raw;
CREATE EXTERNAL TABLE bsky_raw (
  uri            STRING,
  createdAt      STRING,
  indexedAt      STRING,
  text           STRING,
  langs          ARRAY<STRING>,
  did            STRING,
  author_created STRING,
  reply          BOOLEAN,
  likes          BIGINT,
  reposts        BIGINT
)
COMMENT 'Raw Bluesky search results (duplicates across keywords, no cleaning)'
PARTITIONED BY (`date` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/bsky';
MSCK REPAIR TABLE bsky_raw;

-- Curated zone written by scripts/spark_bsky_preprocess.py
DROP TABLE IF EXISTS bsky_posts;
CREATE EXTERNAL TABLE bsky_posts (
  uri                 STRING,
  did                 STRING,
  text                STRING,
  langs               ARRAY<STRING>,
  reply               BOOLEAN,
  likes               BIGINT,
  reposts             BIGINT,
  created_at_raw      STRING,
  indexed_at_raw      STRING,
  author_created_raw  STRING    COMMENT 'unreliable (negative account ages), do not use as a feature',
  created_ts          TIMESTAMP,
  indexed_ts          TIMESTAMP,
  t_event             TIMESTAMP COMMENT 'event time in UTC: createdAt if plausible, else indexedAt',
  lag_min             DOUBLE    COMMENT 'indexedAt - createdAt in minutes',
  time_source         STRING    COMMENT 'createdAt | indexedAt',
  bad_created_at      BOOLEAN,
  reindexed           BOOLEAN   COMMENT 'indexed more than 1 day after creation',
  in_period           BOOLEAN,
  is_clean            BOOLEAN   COMMENT 'createdAt plausible and lag between -5 and +10 min',
  unrelated_candidate BOOLEAN   COMMENT 'text contains an exclusion term (e.g. the game Quake)',
  matched_keywords    ARRAY<STRING>,
  n_copies            BIGINT    COMMENT 'raw rows of this uri',
  file_date           STRING,
  event_date          DATE
)
COMMENT 'Unique Bluesky posts with quality flags (one row per uri)'
PARTITIONED BY (event_month STRING)
STORED AS PARQUET
LOCATION '/curated/bsky/posts';
MSCK REPAIR TABLE bsky_posts;

DROP TABLE IF EXISTS bsky_posts_clean;
CREATE EXTERNAL TABLE bsky_posts_clean LIKE bsky_posts
LOCATION '/curated/bsky/posts_clean';
ALTER TABLE bsky_posts_clean SET TBLPROPERTIES ('comment' = 'In-period posts with is_clean = true');
MSCK REPAIR TABLE bsky_posts_clean;

DROP TABLE IF EXISTS bsky_minute;
CREATE EXTERNAL TABLE bsky_minute (
  `minute`         TIMESTAMP,
  n_posts          BIGINT,
  n_authors        BIGINT,
  n_posts_clean    BIGINT,
  n_authors_clean  BIGINT,
  n_unrelated      BIGINT
)
COMMENT 'Dense per-minute counts of in-period Bluesky posts (UTC)'
STORED AS PARQUET
LOCATION '/curated/bsky/minute';
