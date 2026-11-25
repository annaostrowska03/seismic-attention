# GDELT data source and media-coverage labels

Owner: Gabriela Majstrak (live NiFi flow, Hive tables, labels). Historical backfill: Anna Ostrowska.
Status: MS2. Numbers are from the full history run (USGS and GDELT 2024-01-01 – 2026-11-20); labels are
computed for the study period in `src/seismic_attention/config.py` (2024-01-01 – 2026-01-01).

## 1. What is collected

GDELT 2.0 publishes one Global Knowledge Graph (GKG) file every 15 minutes (96 per day). We keep only
the rows with an earthquake theme: `themes LIKE '%EARTHQUAKE%'`. In our data every kept row has
`NATURAL_DISASTER_EARTHQUAKE`. Per article we keep `gkg_id`, `date` (file time), `source` (domain),
`url`, `themes` and `locations` (places mentioned in the text, with coordinates).

GDELT is the source of the **label**: whether an earthquake got media coverage (section 6).

The proof of concept uses only the history; the live NiFi flow is implemented and tested, but it is kept
for a possible production system.

## 2. Pipeline

```
History (one-off, Anna):
  GDELT GKG files --(scripts/backfill_gdelt.py)--> data/raw/gdelt_history/articles/DAY.jsonl
                                                   data/raw/gdelt_history/files/DAY.csv   (file log)
      --(scripts/load_history.py)--> HDFS /history/{gdelt_history, gdelt_files}  --> Hive gdelt_history, gdelt_files

Live (NiFi, every 15 min):
  lastupdate.txt -> newest GKG zip -> unpack -> EARTHQUAKE rows
      --> HDFS /raw/gdelt/date=YYYY-MM-DD/<stamp>.jsonl        --> Hive gdelt_raw
      \-> Kafka topic "gdelt", one message per article
      file log --> HDFS /raw/gdelt_files/date=YYYY-MM-DD/<stamp>.jsonl  --> Hive gdelt_files_raw

Labels (Spark):
  usgs_history + gdelt_history + gdelt_files --(scripts/spark_coverage_labels.py)-->
      HDFS /curated/coverage_labels (Parquet) --> Hive coverage_labels
```

`<stamp>` is the time of the GDELT file, `yyyyMMddHHmmss` UTC.

**Why NiFi downloads from GDELT itself** (the Bluesky flow reads local files): GDELT needs no login and
has no rate limit, so NiFi can fetch, unpack and filter on its own. The live flow and the backfill
use the same filter and the same fields, so live data and history mean the same thing.

## 3. NiFi flow (`scripts/nifi_gdelt_flow.py`, export `nifi/flows/gdelt.json`)

Process group `gdelt-ingest`, 15 processors and 4 controller services. The script builds the flow
through the NiFi REST API, so everyone gets the same flow with one command.

| Step | Processor | Notes |
|---|---|---|
| 1 | `InvokeHTTP` GET `https://data.gdeltproject.org/gdeltv2/lastupdate.txt`, every 15 min | https: the listed http addresses redirect (301) |
| 2 | `ExtractText` | `gkg_url` = `(https?://\S+\.gkg\.csv\.zip)`, `gdelt_stamp` = file time |
| 3 | `DetectDuplicate` on `gdelt_stamp` (cache on port 4559) | GDELT sometimes lists the same file twice when it is late |
| 4 | `InvokeHTTP` `${gkg_url}`, follow redirects | 4xx → log `missing` |
| 5 | `UnpackContent` (zip) | broken zip → log `broken` |
| 6 | `QueryRecord` (CSV reader, tab, no header → JSON lines) | same rows and fields as `parse_gkg` in `backfill_gdelt.py`; unreadable CSV → log `broken` |
| 7 | `UpdateAttribute` filename `<stamp>.jsonl` → log `ok` with the number of articles | |
| 8 | `RouteOnAttribute` (at least 1 article) → `PutHDFS` + `PublishKafka` | files without articles get only a log line |

File log line: `{"file": "<stamp>", "status": "ok" | "missing" | "broken", "articles": N}`, the same
statuses as the backfill log `files/DAY.csv`.

## 4. Raw records

Article (`/raw/gdelt`, `gdelt_history`):

| Field | Meaning |
|---|---|
| `gkg_id` | GDELT record id; its first 14 characters are the file time |
| `date` | file time (`yyyyMMddHHmmss`); in `gdelt_raw` not a column, see section 5 |
| `source` | domain of the news site |
| `url` | article address |
| `themes` | GDELT themes separated by `;` |
| `locations` | raw: places separated by `;`, fields by `#` (`type#name#country#adm1#lat#lon#id`); in `gdelt_history`: array of `{name, country, latitude, longitude}` |

## 5. Storage

| Hive table (db `seismic`) | HDFS location | Format | Content |
|---|---|---|---|
| `gdelt_raw` | `/raw/gdelt`, partition `date` | JSON lines | live articles |
| `gdelt_files_raw` | `/raw/gdelt_files`, partition `date` | JSON lines | live file log |
| `gdelt_history` | `/history/gdelt_history` | Parquet | backfill articles, `time` = file time |
| `gdelt_files` | `/history/gdelt_files` | Parquet | backfill file log, one row per expected file |
| `coverage_labels` | `/curated/coverage_labels` | Parquet | one row per earthquake (section 6) |

All tables are external: dropping a table does not delete the files. The `date` partition is the UTC
day of the file. Hive does not allow a data column with the partition name, so `gdelt_raw` has no column
`date`; the file time is `substr(gkg_id, 1, 14)`. Layout of all sources: `docs/hdfs_layout.md`.

## 6. Coverage label (`src/seismic_attention/labels.py`)

One rule for the whole project: the label job and `scripts/eda_history.py` both use this module.

**Rule (from MS1).** An earthquake is `covered` if at least one GDELT earthquake article
- has its file time in **[t, t + 48 h]**, both ends included (t = earthquake time, UTC), and
- mentions a place at most **250 km** from the epicentre (Haversine, Earth radius 6371 km).

`n_articles` counts distinct `gkg_id` (not article–place pairs). `min_dist_km` is the nearest matching
place, so a smaller radius r can be checked with `min_dist_km <= r` without a new join.

**Missing files.** A 48 h window should contain 192 files. A missing file can hide an article but can
never add one. The label is given only when at least **173 of the 192 files (90%)** have status `ok`.
Otherwise `covered` is NULL, for covered and not covered earthquakes alike. The alternative (keep every
positive, require a complete window only for negatives) would keep only positives in windows with gaps
and raise the share of covered earthquakes.

**Aftershocks** are flagged, not removed: `aftershock` = an earlier quake (strictly earlier, at most
48 h before) with the same or larger magnitude within 250 km. Articles about the main shock would make
an aftershock look covered.

**Join.** Quakes and article places are joined on (calendar day, latitude band of 250/111 degrees),
with the neighbouring bands. A 48 h window touches at most 3 days, and a band wider than 250 km means
a match is always in the same or a neighbouring band, so Spark can hash-join instead of comparing
every pair.

**Period.** Earthquakes are taken from the study period; GDELT articles and files up to
`PERIOD_END + 48 h`, so the window of a quake on the last day is complete.

Columns of `coverage_labels`: `id, time, mag, latitude, longitude, depth_km, place` (from
`usgs_history`, `type = 'earthquake'`, one row per `id`), `n_articles, min_dist_km, files_ok,
gdelt_complete, aftershock, covered` (true / false / NULL).

## 7. How to run

From the repository root, Docker stack running (`docs/SETUP.md`).

```bash
# live flow
python scripts/nifi_gdelt_flow.py create
python scripts/nifi_gdelt_flow.py start
python scripts/nifi_gdelt_flow.py status
python scripts/nifi_gdelt_flow.py stop
python scripts/nifi_gdelt_flow.py export      # after changes in the UI: save nifi/flows/gdelt.json

# Hive tables over the live raw zone, and the check that articles agree with the file log
python scripts/hive_gdelt.py create
python scripts/hive_gdelt.py verify

# history (files in data/raw/gdelt_history and data/raw/usgs_history) and labels
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/load_history.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_coverage_labels.py

# tests
python scripts/run_spark_tests.py
```

In Git Bash, prefix the `docker compose exec` commands with `MSYS_NO_PATHCONV=1`.

## 8. Control numbers

History (`load_history.py`, 2024-01-01 – 2026-11-20):

| Number | Value |
|---|---|
| `gdelt_history` articles | 465 320 |
| `gdelt_files` rows (1055 days × 96) | 101 280 |
| … `ok` / `missing` | 99 595 / 1 685 (1.7%; 1 569 in June 2025, 105 in July 2025) |
| `usgs_history` events | 22 033 |

Labels (`coverage_labels`, 2024–2025, 14 956 earthquakes):

| covered | not aftershock | aftershock |
|---|---|---|
| true | 4 915 | 4 046 |
| false | 4 222 | 1 382 |
| NULL | 260 | 131 |

Not aftershocks with a label: 9 137, of which 53.8% covered. NULL: 391 (2.6%), of them 376 in June 2025.
Cross-check: `quake_coverage` from `eda_history.py` (independent run on data to 2026-11) agrees
for all 14 956 earthquakes in `n_articles`, `min_dist_km`, `gdelt_complete` and `aftershock`.

## 9. Known data issues and limitations

- **Article time is the file time** (15-minute resolution), not the publication time.
- **One automated site dominates part of the period.** `earthquakenewstoday.com` publishes a short
  article for almost every earthquake. It is the second largest source (32 562 of 465 320 articles) and appears
  in GDELT only from May 2024 to December 2025. Covered share without it: 34.9% instead of 53.8%,
  and the month-to-month level stays flat (about 25–40%). With it, the level jumps from about 36% (January–April
  2024) to about 57%. Whether to exclude automated sources is an open decision; the table keeps all sources.
- **GDELT is not all media.** "Not covered" means no GDELT article with a place within 250 km.
  Earthquakes far from any named place (e.g. mid-ocean ridges) are almost never covered.
- **The aftershock rule uses the distance to the epicentre**, so it misses aftershocks of very large
  earthquakes: M4.9 `us7000pnb6`, 25 minutes after the M7.7 in Myanmar (28.03.2025), is 322 km from its
  epicentre (the rupture was about 450 km long) and is not flagged.
- **Start of the period:** earthquakes in early January 2024 cannot see main shocks from December 2023
  (no data).
- **The live log only knows the files it asked for.** If NiFi or the laptop is off, there is no log
  line at all; gaps are found by comparing with the 96 expected files per day.
- **Labels are computed for the history only.** `usgs_raw` (live) holds several versions of one event;
  choosing the latest version for live labels is future work.

## 10. Operational notes

- NiFi 1.28: `Follow Redirects` takes `True` / `False` with a capital letter; the CSV reader needs a
  non-empty `Quote Character` (the flow uses `¤`); empty CSV fields become `null`, so the SQL uses
  `COALESCE` to match the backfill (`''`); field trimming must be off.
- `DetectDuplicate` uses its own cache on port 4559 (the USGS flow uses 4558).
- Kafka: count messages with a `read_committed` consumer (NiFi publishes in transactions).
- Hive: `date`, `time` and `file` are reserved words and need backticks; queries with `JOIN` need
  `SET hive.auto.convert.join=false;` (otherwise `MapredLocalTask` fails).
- The Spark containers run Python 3.8.

## 11. Test cases

IDs as in the MS2 test documentation ([testing.md](testing.md)). F = functional (`pytest`, every pull
request in GitHub Actions and `python scripts/run_spark_tests.py`), I = integration, Q = data quality.
The history download and loading (HIST-*) are listed in [testing.md](testing.md).

| ID | Objective | Steps | Expected result | Actual result |
|---|---|---|---|---|
| GDELT-F-01 | The flow takes only the GKG line of `lastupdate.txt` | `test_gkg_url_is_the_gkg_line_only`, `test_https_url_is_accepted`, `test_no_gkg_line_gives_no_match` | GKG address matched, other lines not | passed |
| GDELT-F-02 | File time read from the URL | `test_gdelt_stamp_is_the_time_of_the_gkg_file`, `test_translated_file_has_no_stamp` | stamp `yyyyMMddHHmmss`; translated files ignored | passed |
| GDELT-F-03 | Raw zone follows the team convention | `test_hdfs_directories_follow_the_team_convention` | `/raw/gdelt/date=`, `/raw/gdelt_files/date=` | passed |
| LAB-F-01 | Distance between two points | `tests/test_labels.py::test_distance_*` (3 tests) | 0 for the same point; ≈111 km per degree of latitude; shorter for longitude near the poles | passed |
| LAB-F-02 | Article places | `test_article_places_drops_places_without_coordinates_and_duplicates` | places without coordinates and duplicates dropped | passed |
| LAB-F-03 | Matching articles with earthquakes | `test_match_*` (5 tests) | both ends of the 48 h window included; 250 km limit (249 vs 251 km); articles counted, not places; nearest place taken; band border and midnight handled | passed |
| LAB-F-04 | Completeness of the GDELT window | `test_window_files_counts_only_ok_files_in_window`, `test_full_window_has_193_files_with_both_ends`, `test_label_threshold_173_files` | only `ok` files in the window counted; label from 173 files | passed |
| LAB-F-05 | Unknown label for incomplete windows | `test_incomplete_window_gives_null_also_for_covered_quake`, `test_quake_without_files_or_articles` | NULL below 173 files, also when an article was found | passed |
| LAB-F-06 | Aftershock flag | `test_aftershocks`, `test_aftershock_flag_in_coverage` | flag only for a strictly earlier quake, at most 48 h before, magnitude ≥, within 250 km | passed |
| LAB-F-07 | Other earthquake columns kept | `test_coverage_keeps_extra_quake_columns` | input columns (e.g. `place`) are in the output | passed |
| LAB-F-08 | The label tests catch errors | 6 deliberate changes in `labels.py`, one at a time (see below), run the tests | each change makes a test fail | 6/6 caught (21.11) |
| GDELT-I-01 | Flow output equals the backfill | flow and `parse_gkg` of `backfill_gdelt.py` on the same 2 live files | identical rows and fields | identical (21.11) |
| GDELT-I-02 | Articles in HDFS and Kafka | `hdfs dfs -ls -R /raw/gdelt`; `read_committed` consumer on topic `gdelt` | file in the `date=` folder of the file time; one valid JSON message per article | as expected (21.11) |
| GDELT-I-03 | Duplicate and missing files | run the flow twice on the same file; request a file that does not exist | second run dropped by `DetectDuplicate`; HTTP 404 gives a `missing` log line | as expected (21.11) |
| GDELT-Q-01 | Raw articles agree with the file log | `python scripts/hive_gdelt.py verify` | all checks OK | 6/6 OK (21.11) |
| LAB-I-01 | Label table built | `spark-submit scripts/spark_coverage_labels.py` | one row per earthquake of the study period; table `seismic.coverage_labels` | 14 956 earthquakes, 391 with NULL label (22.11) |
| LAB-Q-01 | Labels agree with an independent run | full outer join of `coverage_labels` and `quake_coverage` of `eda_history.py`, limited to 2024–2025 | 0 differences in `n_articles`, `min_dist_km`, window completeness and aftershock flag | 0 after the end-of-period fix (first 27 differences on 30–31.12.2025) (22.11) |
| LAB-Q-02 | One row per earthquake, NULL only for incomplete windows | Hive: rows, distinct `id`, NULL labels, incomplete windows of `coverage_labels` | rows = distinct ids; NULL labels = incomplete windows | 14 956 = 14 956; 391 = 391 (22.11) |

Strength of `tests/test_labels.py` (LAB-F-08) was checked with 6 deliberate changes in `labels.py`
(`<` instead of `<=` at the window end, the rejected gap rule that keeps positives in incomplete windows,
threshold 172, no neighbouring bands, `count` instead of `countDistinct`, `>` instead of `>=` for
aftershocks); each one makes the matching test fail. Not tested: the `broken` status of the live flow
(hard to produce).

Logs: [test_evidence/team/](test_evidence/team/README.md).
