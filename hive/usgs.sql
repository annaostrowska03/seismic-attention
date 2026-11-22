-- External Hive table for the USGS raw zone (database "seismic").
-- Run: python scripts/hive_usgs.py create
-- Dropping an EXTERNAL table never deletes its files in HDFS, so this script is safe to re-run.

CREATE DATABASE IF NOT EXISTS seismic;
USE seismic;

-- Written by NiFi (nifi/flows/usgs.json): one GeoJSON feature per file,
-- /raw/usgs/date=YYYY-MM-DD/usgs-<ingest time>-<id>-<updated>.json, date = UTC day of ingest.
-- The columns follow the JSON 1:1; only the fields used by the project are listed (Hive ignores the rest).
DROP TABLE IF EXISTS usgs_raw;
CREATE EXTERNAL TABLE usgs_raw (
  id              STRING COMMENT 'USGS event id',
  properties      STRUCT<
                    mag:     DOUBLE,
                    magType: STRING,
                    place:   STRING,
                    `time`:  BIGINT COMMENT 'origin time, ms since 1970 UTC',
                    updated: BIGINT COMMENT 'last update of the event at USGS, ms since 1970 UTC',
                    type:    STRING,
                    status:  STRING COMMENT 'automatic | reviewed | deleted',
                    net:     STRING,
                    sig:     INT,
                    tsunami: INT,
                    felt:    INT,
                    alert:   STRING>,
  geometry        STRUCT<coordinates: ARRAY<DOUBLE> COMMENT '[longitude, latitude, depth_km]'>,
  ingest_time_utc STRING COMMENT 'when NiFi saw this version, yyyy-MM-dd HH:mm:ss.SSS UTC'
)
COMMENT 'Raw USGS M4.5+ events from the live feed; one row per new version (id, updated) of an event'
PARTITIONED BY (`date` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/usgs';
MSCK REPAIR TABLE usgs_raw;

-- Flat view with the column names of usgs_history (scripts/load_history.py).
-- Still one row per version: the same id can appear several times with a different "updated".
-- ms -> TIMESTAMP keeps the milliseconds (a direct numeric cast is blocked by hive.strict.timestamp.conversion);
-- from_unixtime uses the server time zone, which is UTC in this stack.
DROP VIEW IF EXISTS usgs_raw_flat;
CREATE VIEW usgs_raw_flat AS
SELECT
  id,
  CAST(concat(from_unixtime(properties.`time` DIV 1000), '.', lpad(CAST(properties.`time` % 1000 AS STRING), 3, '0'))
       AS TIMESTAMP) AS `time`,
  CAST(concat(from_unixtime(properties.updated DIV 1000), '.', lpad(CAST(properties.updated % 1000 AS STRING), 3, '0'))
       AS TIMESTAMP) AS updated,
  properties.mag             AS mag,
  properties.magType         AS mag_type,
  properties.place           AS place,
  geometry.coordinates[1]    AS latitude,
  geometry.coordinates[0]    AS longitude,
  geometry.coordinates[2]    AS depth_km,
  properties.type            AS type,
  properties.status          AS status,
  properties.net             AS net,
  properties.sig             AS sig,
  properties.tsunami         AS tsunami,
  properties.felt            AS felt,
  properties.alert           AS alert,
  ingest_time_utc,
  `date`
FROM usgs_raw;
