# USGS data source

Owner: Anna Ostrowska (live NiFi flow, history download, feature module). Hive table over the live raw
zone: Gabriela Majstrak. Status: MS2.

## 1. What is collected

USGS publishes every earthquake it detects. We use two ways of getting them:

- **Live feed** – GeoJSON feed of M4.5+ events of the last hour
  (`https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson`), read by NiFi every 60 s.
- **History** – the USGS catalogue through the FDSN event API
  (`https://earthquake.usgs.gov/fdsnws/event/1/query`), M4.5+, downloaded month by month for
  2024-01-01 – 2026-11-20.

The proof of concept uses only the history; the live flow is implemented and tested, but it is kept for a
possible production system.

One record is one earthquake (GeoJSON `Feature`). The earthquake is the central entity of the project: every
coverage label, Bluesky reaction window and model score belongs to one USGS event `id`.

## 2. Pipeline

```
Live (NiFi, every 60 s):
  4.5_hour.geojson -> split events -> drop versions already seen (id + updated) -> add ingest_time_utc
      --> Kafka topic "usgs", key = event id
      \-> HDFS /raw/usgs/date=YYYY-MM-DD/usgs-<ingest time>-<id>-<updated>.json   --> Hive usgs_raw, view usgs_raw_flat

History (one-off):
  FDSN event API --(scripts/backfill_usgs.py)--> data/raw/usgs_history/YYYY-MM.jsonl
      --(scripts/load_history.py, Spark)--> HDFS /history/usgs_history (Parquet) --> Hive usgs_history
```

## 3. NiFi flow (`nifi/flows/usgs.json`, process group `USGS ingest`)

| Step | Processor | Notes |
|---|---|---|
| 1 | `InvokeHTTP` GET `4.5_hour.geojson`, every 60 s | the feed covers only the last hour |
| 2 | `SplitJson` `$.features` | one flow file per event |
| 3 | `EvaluateJsonPath` | attributes `usgs.id`, `usgs.updated` |
| 4 | `DetectDuplicate` on `${usgs.id}_${usgs.updated}` | cache server on port 4558, entries kept 7 days, saved on disk, so a NiFi restart does not send the last hour again |
| 5 | `JoltTransformJSON` | adds `ingest_time_utc` (`yyyy-MM-dd HH:mm:ss.SSS`, UTC) = first-seen time of this version |
| 6 | `PublishKafka_2_6` topic `usgs`, key = event id, in transactions | failures are retried |
| 7 | `UpdateAttribute` | file name `usgs-<ingest time>-<id>-<updated>.json` |
| 8 | `PutHDFS` `/raw/usgs/date=${now():format('yyyy-MM-dd', 'UTC')}` | failures are retried |

A new version of an event (same `id`, newer `updated`, e.g. a revised magnitude) passes the duplicate
check and is stored again; the first-seen time of an event is the smallest `ingest_time_utc` of its `id`.

## 4. Raw record (fields used by the project)

| Field | Meaning |
|---|---|
| `id` | event id, e.g. `us7000pnb6` |
| `properties.time`, `properties.updated` | origin time and time of the last revision, epoch milliseconds UTC |
| `properties.mag`, `properties.magType` | magnitude and the method (`mb`, `mww`, `mwr`, `ml`, …) |
| `properties.place` | location text, e.g. `72 km SSE of Ancud, Chile` or `south of the Fiji Islands` |
| `geometry.coordinates` | `[longitude, latitude, depth_km]` |
| `properties.type` | `earthquake`, `landslide`, `quarry blast`, … |
| `properties.status` | `automatic` or `reviewed` |
| `properties.net`, `sig`, `tsunami`, `felt`, `alert` | network; significance, tsunami flag, felt reports and PAGER alert (filled later) |
| `ingest_time_utc` | live flow only: when NiFi first saw this version |

## 5. History download

`scripts/backfill_usgs.py` (Python, `requests`) queries the FDSN API for one month at a time, because one
query returns at most 20 000 events; it writes one file per month (`data/raw/usgs_history/YYYY-MM.jsonl`,
one feature per line). `scripts/load_history.py` (Spark) converts it to the table `usgs_history`: epoch
milliseconds to UTC timestamps, coordinates and depth taken out of the geometry, numeric types fixed
(USGS sends whole numbers as integers).

## 6. Storage

| Hive table (db `seismic`) | HDFS location | Format | Content |
|---|---|---|---|
| `usgs_raw` | `/raw/usgs`, partition `date` (UTC day of ingest) | JSON | live events, one row per version, nested as received |
| `usgs_raw_flat` (view) | – | – | `usgs_raw` with the column names of `usgs_history` |
| `usgs_history` | `/history/usgs_history` | Parquet | catalogue 2024-01-01 – 2026-11-20, one row per event |

DDL: `hive/usgs.sql` (`python scripts/hive_usgs.py create`); `usgs_history` is created by `load_history.py`.

## 7. Features for the coverage model (`src/seismic_attention/features.py`)

`usgs_features()` builds the T0 features of one event; the same function is used by the batch jobs (as a
Spark UDF, `SPARK_SCHEMA`) and will be used in the stream:

| Feature | Rule |
|---|---|
| `mag`, `depth_km`, `latitude`, `longitude` | from the record, as float |
| `hour_utc`, `weekday` | from the origin time (UTC; Monday = 0) |
| `hour_local` | approximate local solar hour: UTC hour + longitude / 15 |
| `place_distance_km`, `place_name`, `place_region` | parsed from `place`: `72 km SSE of Ancud, Chile` -> (72, Ancud, Chile); a sea area or ridge gives only a region |

## 8. How to run

```bash
# live flow: import nifi/flows/usgs.json in the NiFi UI (nifi/README.md), then
python scripts/hive_usgs.py create      # Hive table + view over /raw/usgs (re-run to see new days)
python scripts/hive_usgs.py verify      # checks of usgs_raw
python scripts/verify_usgs_ingest.py    # HDFS files vs Kafka messages of the flow

# history
python scripts/backfill_usgs.py --start 2024-01-01 --end 2026-11-21
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/load_history.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/verify_history.py
```

## 9. Control numbers

| Number | Value |
|---|---|
| `usgs_history` rows (2024-01-01 – 2026-11-20) | 22 033, no duplicate `id` |
| … in the study period 2024–2025 | 14 958 (14 956 earthquakes, 2 landslides) |
| … per day (2024–2025) | 20.5 on average, 3 – 422 (max on 2025-07-30, Kamchatka sequence) |
| … magnitude type (2024–2025) | `mb` 11 890, `mww` 2 646, `mwr` 345, `ml` 40, other 37 |
| … status | all `reviewed` |
| … missing `felt` / `alert` (2024–2025) | 81.8% / 91.1% |

Sources: `docs/test_evidence/team/04_usgs_gdelt_period_stats.txt`, `docs/test_evidence/team/06_verify_history.txt`.

## 10. Known data issues and limitations

- **The catalogue keeps only the final, reviewed version of each event.** All 14 958 records of 2024–2025
  have the status `reviewed`, and their `updated` lies 60 to 1 051 days after the origin time (median
  73.5 days). The first, automatic version that a live system sees at T0, and the time of its
  publication, are not in the history. Training and the planned replay therefore use slightly more
  accurate T0 values than a live system would have, and the M4.5 threshold is applied to the final
  magnitude (3 984 events of 2024–2025 have exactly M4.5). The live flow keeps every version, so the size
  of the revisions can be measured in MS3.
- **The feed covers only the last hour.** Events published while NiFi is stopped for longer are missing
  in the live data (they are still in the catalogue).
- **Named events have a title instead of a place.** For 16 of the 14 958 events of 2024–2025, all well-known
  earthquakes, `place` is the event name, e.g. `2024 Noto Peninsula, Japan Earthquake` or
  `2025 Kamchatka Peninsula, Russia Earthquake`. `parse_place` then returns no distance and the region
  `Japan Earthquake`, so these events fall into the "no named place" group of the analysis. The share is
  small (0.1%), but these are the most covered events; the parser will handle this form in MS3.
- **The `date` partition of `usgs_raw` is the UTC day of ingest**, not of the earthquake.
- `sig`, `felt` and `alert` are filled later and are not used as features.

## 11. Test cases

IDs as in the MS2 test documentation. F = functional (`pytest`), I = integration, Q = data quality.

| ID | Objective | Steps | Result |
|---|---|---|---|
| USGS-F-01 | Parse the `place` text | `tests/test_features.py::test_parse_place` (11 cases) | passed |
| USGS-F-02 | Local solar hour | `test_local_hour` | passed |
| USGS-F-03 | Feature record and Spark schema | `test_usgs_features`, `test_spark_schema_matches_feature_names` | passed |
| USGS-I-01 | NiFi output in HDFS and Kafka | `python scripts/verify_usgs_ingest.py` | 5/5 OK (55 files, 71 records, 22.11) |
| USGS-I-02 | A new earthquake goes through the flow | wait for a new M4.5+ event; list `/raw/usgs`; read topic `usgs` | event in HDFS and Kafka (16.11) |
| USGS-I-03 | A NiFi restart does not resend events | restart NiFi while the feed has not changed | no new files (16.11) |
| USGS-I-04 | The flow can be imported | upload `usgs.json`, enable the 2 cache services | all processors valid (16.11) |
| USGS-Q-01 | Raw table valid | `python scripts/hive_usgs.py verify` on test records in the NiFi format | 5/5 OK (21.11) |
| USGS-Q-02 | `verify` detects an error | add a copy of one file (same version twice), run `verify` | FAIL and exit code 1, as expected (21.11) |

History download and loading (HIST-*) are listed in [testing.md](testing.md).
