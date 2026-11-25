# Testing

How the data acquisition and pre-processing modules are tested, how to run the tests, and the list of
all test cases with their IDs. The same IDs are used in the MS2 test documentation, in the source documents
([usgs.md](usgs.md), [gdelt.md](gdelt.md), [bluesky.md](bluesky.md), [streaming.md](streaming.md)) and in
the logs in [test_evidence/](test_evidence/README.md).

## 1. Test types

| Type | Letter | What | How |
|---|---|---|---|
| Functional | F | single functions and modules: parsing, API client, backfill logic, Spark transformations, label rule, streaming deduplication | `pytest`, fake HTTP responses and small Spark data frames; no network, no running stack |
| Integration | I | the pipeline on the Docker stack with real data: NiFi → Kafka/HDFS, Spark jobs, Hive tables | scripts in `scripts/` (commands below) |
| Quality | Q | the output of the transformations: duplicates, value ranges, completeness, counts equal to the input, stream equal to batch | `verify` scripts, Hive queries, control numbers of the Spark jobs |

## 2. How to run

```bash
pytest                                   # host, Python 3.11+; Spark tests are skipped without Java
python scripts/run_spark_tests.py        # all tests in a one-off Spark container (Java, Python 3.8)
python scripts/run_spark_tests.py -k labels   # extra arguments go to pytest
```

GitHub Actions (`.github/workflows/tests.yml`) runs all tests on every pull request and on every push to
`main`, with Java, so the Spark tests run too; the job fails if any test is skipped.

Integration and quality checks need the stack (`docker compose up -d`, [SETUP.md](SETUP.md)) and the data;
they are run from the repository root:

```bash
python scripts/smoke_test.py                 # ENV-I-01
python scripts/verify_usgs_ingest.py         # USGS-I-01
python scripts/hive_usgs.py verify           # USGS-Q-01
python scripts/hive_gdelt.py verify          # GDELT-Q-01
python scripts/verify_bsky_ingest.py         # BSKY-I-01, BSKY-I-02
python scripts/hive_bsky.py verify           # BSKY-I-04, BSKY-Q-03, BSKY-Q-04
python scripts/bsky_backfill.py --dry-run    # BSKY-Q-02
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/load_history.py          # HIST-I-03
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/verify_history.py        # HIST-Q-01..04
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_coverage_labels.py # LAB-I-01
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_bsky_preprocess.py # BSKY-I-03, BSKY-Q-01, BSKY-Q-05
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_bsky_streaming.py  # STR-I-01 (options: streaming.md)
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/compare_stream_batch.py  # STR-Q-01
```

In Git Bash, prefix the `docker compose exec` commands with `MSYS_NO_PATHCONV=1`.

## 3. Test IDs

`<AREA>-<type>-<number>`, e.g. `BSKY-Q-02`.

| Area | Module | Owner |
|---|---|---|
| ENV, CI | Docker stack, full test suite, GitHub Actions | Gabriela (stack), Michał (CI) |
| USGS | live NiFi flow, Hive table `usgs_raw`, feature module | Anna, Gabriela (Hive) |
| HIST | history download (USGS, GDELT), loading to HDFS/Hive, checks of the history | Anna |
| GDELT | live NiFi flow, Hive tables `gdelt_raw`, `gdelt_files_raw` | Gabriela |
| LAB | coverage labels | Gabriela |
| BSKY | Bluesky backfill, NiFi ingestion, Spark pre-processing, Hive tables | Michał |
| STR | Spark Structured Streaming on the `bsky` topic | Norbert |

## 4. Automated functional tests (82)

| Test file | Tests | IDs |
|---|---:|---|
| `tests/test_features.py` | 14 (`test_parse_place` has 11 cases) | USGS-F-01 … USGS-F-03 |
| `tests/test_backfill.py` | 7 (`test_fetch_file_status` has 3 cases) | HIST-F-01 … HIST-F-04 |
| `tests/test_load_history.py` | 4 | HIST-F-05 … HIST-F-07 |
| `tests/test_nifi_gdelt_flow.py` | 6 | GDELT-F-01 … GDELT-F-03 |
| `tests/test_labels.py` | 17 | LAB-F-01 … LAB-F-07 |
| `tests/test_bluesky_api.py` | 22 | BSKY-F-01 … BSKY-F-10, BSKY-F-15 |
| `tests/test_bluesky_preprocess.py` | 11 | BSKY-F-11 … BSKY-F-16 |
| `tests/test_bsky_streaming.py` | 1 | STR-F-01 |
| **Total** | **82** | last run: 82 passed ([test_evidence/pytest_all.txt](test_evidence/pytest_all.txt)) |

| ID | Objective | Tests |
|---|---|---|
| USGS-F-01 | Parse the USGS `place` text | `test_parse_place` |
| USGS-F-02 | Local solar hour | `test_local_hour` |
| USGS-F-03 | Feature record and Spark schema | `test_usgs_features`, `test_spark_schema_matches_feature_names` |
| HIST-F-01 | Split the period into monthly USGS queries | `test_month_ranges_across_new_year`, `test_month_ranges_part_of_month` |
| HIST-F-02 | GDELT file names of a day | `test_day_stamps` |
| HIST-F-03 | Keep only earthquake articles | `test_parse_gkg_keeps_only_earthquake_articles` |
| HIST-F-04 | Status of a GDELT file | `test_fetch_file_status` (ok / missing / broken) |
| HIST-F-05 | USGS record to table row | `test_usgs_coordinates_time_and_types` |
| HIST-F-06 | GDELT article to table row | `test_gdelt_locations_and_time`, `test_gdelt_without_locations` |
| HIST-F-07 | GDELT file log types | `test_file_log_types` |
| GDELT-F-01 | Only the GKG line of `lastupdate.txt` | `test_gkg_url_is_the_gkg_line_only`, `test_https_url_is_accepted`, `test_no_gkg_line_gives_no_match` |
| GDELT-F-02 | File time read from the URL | `test_gdelt_stamp_is_the_time_of_the_gkg_file`, `test_translated_file_has_no_stamp` |
| GDELT-F-03 | HDFS folders follow the team convention | `test_hdfs_directories_follow_the_team_convention` |
| LAB-F-01 … LAB-F-07 | coverage label rule | see [gdelt.md](gdelt.md), section 11 |
| BSKY-F-01 … BSKY-F-16 | Bluesky client, backfill and pre-processing | see [bluesky.md](bluesky.md), section 11 |
| STR-F-01 | Deduplication survives a restart of the streaming query | `test_dedup_survives_restart` |

`LAB-F-08` is a manual check of the label tests themselves (6 deliberate errors in `labels.py`, each
caught by a test).

## 5. Integration and quality checks

| ID | Objective | Result | Evidence |
|---|---|---|---|
| ENV-I-01 | Docker stack healthy | 6/6 services OK | `bluesky/01_smoke_test.txt` |
| CI-01 | All automated tests pass in the Spark container | 82 passed (25.11) | `pytest_all.txt` |
| CI-02 | All tests pass in GitHub Actions, none skipped | passed on every pull request; run #28 on `main` (merge of PR #11) | `bluesky/ci_run.png` |
| USGS-I-01 … USGS-I-04 | live flow: HDFS vs Kafka, new event, restart, import | passed (16.–22.11) | [usgs.md](usgs.md), section 11 |
| USGS-Q-01, USGS-Q-02 | `usgs_raw` valid; `verify` detects an error | passed (21.11) | [usgs.md](usgs.md), section 11 |
| HIST-I-01 | USGS history download | 35 monthly files | – |
| HIST-I-02 | GDELT download can be resumed | finished days skipped | – |
| HIST-I-03 | Load the history to HDFS and Hive | 22 033 / 465 320 / 101 280 rows | `team/01_load_history.txt` |
| HIST-I-04 | Exploratory analysis on the full history | `report.md`, 15 CSV tables and `quake_coverage` written (22.11) | `data/processed/eda_history/` (not in Git) |
| HIST-Q-01 | Hive has every downloaded record | 3/3 OK | `team/06_verify_history.txt` |
| HIST-Q-02 | USGS values valid | 6/6 OK | `team/06_verify_history.txt` |
| HIST-Q-03 | GDELT articles valid | 3/4 OK; 5 duplicate ids, an error of the source | `team/06_verify_history.txt` |
| HIST-Q-04 | GDELT file log complete | 3/3 OK | `team/06_verify_history.txt` |
| HIST-Q-05 | Statistics of the study period | USGS ids unique; 70 176 GDELT files = expected | `team/04_usgs_gdelt_period_stats.txt` |
| GDELT-I-01 … GDELT-I-03, GDELT-Q-01 | live GDELT flow and Hive tables | passed (21.11) | [gdelt.md](gdelt.md), section 11 |
| LAB-I-01, LAB-Q-01, LAB-Q-02 | label table and its checks | passed (22.11) | `team/02_coverage_labels.txt`, `team/03_hive_label_checks.txt` |
| BSKY-I-01 … BSKY-I-04, BSKY-Q-01 … BSKY-Q-05 | Bluesky ingestion, Spark job, Hive tables | passed (21.–22.11) | `bluesky/` |
| STR-I-01 | Streaming job reads topic `bsky` | 99 micro-batches, 1 973 106 messages | `team/05_stream_vs_batch.txt` |
| STR-Q-01 | Stream counts equal batch counts | 0 of 539 714 minutes differ | `team/05_stream_vs_batch.txt` |

Paths in the last column are relative to [test_evidence/](test_evidence/README.md).

## 6. Not covered yet

- The `broken` status of the live GDELT flow (hard to produce on purpose).
- The live USGS and GDELT flows are checked on the days they ran (from 16.11.2026), not on the whole period.
- Bot filter, burst detection, models, serving layer and the simulated run: MS3/MS4.
