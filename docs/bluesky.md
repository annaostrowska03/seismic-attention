# Bluesky data source

Owner: Michał Kukla. Status: MS2. Control numbers and test results are filled in after the full
2024–2025 run.

## 1. What is collected

Public Bluesky posts that mention earthquakes, for the study period **2024-01-01 – 2025-12-31 (UTC)**.
Posts are found with the search endpoint `app.bsky.feed.searchPosts` using 22 keywords in several
languages (`src/seismic_attention/ingest/bluesky_keywords.json`). The same file holds exclusion
terms (mostly the video game *Quake*); they only flag posts, nothing is deleted.

The period is set in `src/seismic_attention/config.py` and used by both the download and the Spark job.

## 2. Pipeline

```
searchPosts API --(Python backfill)--> data/raw/bsky/YYYY-MM-DD.jsonl   (local, not in Git)
      --(NiFi)--> HDFS /raw/bsky/date=YYYY-MM-DD/          raw zone, whole daily files
               \-> Kafka topic "bsky"                       one message per post (replay path)
      --(Spark)--> HDFS /curated/bsky/{posts, posts_clean, minute}   Parquet
      --(Hive)--> database "seismic": bsky_raw, bsky_posts, bsky_posts_clean, bsky_minute
```

**Why the download is in Python and not in NiFi.** The historical download needs a login token
that is reused for hours, paging with a cursor, splitting busy time windows and resuming after an
interruption. This is simple in Python and awkward in NiFi. NiFi takes over from the local files:
it loads them into HDFS and Kafka, and it is the entry point for the replay demo.

## 3. Acquisition

- Login with an app password (`BSKY_HANDLE`, `BSKY_APP_PASSWORD` in `.env`, never committed).
  Requests go through the account's server (`bsky.social`) with the `atproto-proxy` header; the
  public endpoint refused search requests.
- Rate limits: at most about 4 requests per second (limit is ~3000 per 5 minutes); one login per
  run, a new login only when the token expires.
- Each keyword is fetched in 6-hour windows. The API returns at most 100 pages of 100 posts, so a
  window with more results is split in half until it fits (down to 1 minute).
- Finished windows are written to `data/raw/bsky/_done.txt`. A new run skips every window that is
  already covered, so running the script again downloads only what is missing. To download
  everything again, move `data/raw/bsky` away first.
- Posts are appended to one file per day (date of the window start). A post that matches several
  keywords appears several times in the raw files; duplicates are removed in Spark.

Commands: see section 7.

## 4. Raw record (one JSON line)

| Field | Type | Meaning |
|---|---|---|
| `uri` | string | Unique post id (`at://<did>/app.bsky.feed.post/<id>`) |
| `createdAt` | string, ISO 8601 | Time set by the author's app – not reliable |
| `indexedAt` | string, ISO 8601 | Time Bluesky indexed the post – set by the server |
| `text` | string | Post text |
| `langs` | array of strings | Languages declared by the author, can be null |
| `did` | string | Author's account id |
| `author_created` | string, ISO 8601 | Account creation time from the profile – not reliable |
| `reply` | boolean | The post is a reply |
| `likes`, `reposts` | integer | Counts at download time, not at posting time |

No handle, display name or other profile data is stored.

## 5. Processing rules (Spark, `src/seismic_attention/preprocess/bluesky.py`)

Rows are never deleted; every unique post is kept with its flags.

| Column | Rule |
|---|---|
| deduplication | one row per `uri`; the copy from the earliest daily file is kept, `n_copies` = number of raw rows |
| `created_ok` (internal) | `createdAt` is inside the study period and at most 5 min after `indexedAt` |
| `t_event` | `createdAt` if `created_ok`, otherwise `indexedAt` (UTC) |
| `time_source` | `createdAt` or `indexedAt` – where `t_event` comes from |
| `bad_created_at` | not `created_ok` |
| `lag_min` | `indexedAt − createdAt` in minutes |
| `reindexed` | `created_ok` and `lag_min` > 1 day |
| `in_period` | `t_event` inside the study period |
| `is_clean` | `created_ok` and −5 ≤ `lag_min` ≤ 10 (what a live system would see in time) |
| `unrelated_candidate` | text contains an exclusion term (flag only) |
| `matched_keywords` | search keywords found in the text (case-insensitive substring) |
| `event_month` | `yyyy-MM` of `t_event`; posts outside the period go to `out_of_period` |

Per-minute table: one row for every minute of the period (also minutes with no posts), with
`n_posts`, `n_authors`, `n_posts_clean`, `n_authors_clean`, `n_unrelated`.

## 6. Storage

| Hive table (db `seismic`) | HDFS location | Format | Content |
|---|---|---|---|
| `bsky_raw` | `/raw/bsky`, partition `date` | JSON lines | raw files as loaded by NiFi, with duplicates |
| `bsky_posts` | `/curated/bsky/posts`, partition `event_month` | Parquet | unique posts with flags |
| `bsky_posts_clean` | `/curated/bsky/posts_clean`, partition `event_month` | Parquet | in-period posts with `is_clean` |
| `bsky_minute` | `/curated/bsky/minute` | Parquet | per-minute counts |

All tables are external: dropping a table does not delete the files. Kafka topic `bsky`
(3 partitions) holds one raw JSON line per message, without a key.

## 7. How to run

From the repository root, Docker stack running (`docs/SETUP.md`).

```powershell
# 1. download (resumable; without parameters = whole study period, only missing windows)
.\.venv\Scripts\python.exe scripts\bsky_backfill.py --dry-run
.\scripts\run_bsky_backfill.ps1

#    keyword review: posts per keyword, what each keyword adds, example texts
python scripts/bsky_keyword_stats.py

# 2. NiFi flow: local files -> HDFS + Kafka (can also be started from the NiFi canvas)
python scripts/nifi_bsky_flow.py create
python scripts/nifi_bsky_flow.py start
python scripts/nifi_bsky_flow.py status
#    when all files are processed: HDFS and Kafka compared with the local files
python scripts/verify_bsky_ingest.py

# 3. Spark job: HDFS raw -> curated Parquet, writes data/processed/bsky_summary.json
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_bsky_preprocess.py

# 4. Hive tables and count check against the Spark summary
python scripts/hive_bsky.py create
python scripts/hive_bsky.py verify

# 5. tests (Spark tests need Java; this runs everything in the Spark container)
python scripts/run_spark_tests.py

# 6. sample for the repository: about 1000 raw records with the edge cases, fixed seed
python scripts/make_bsky_sample.py
```

## 8. Control numbers (2024–2025)

Source: `data/processed/bsky_summary.json` from the Spark job on HDFS.

| Number | Value |
|---|---|
| raw rows / daily files | – |
| unique posts | – |
| posts in the study period | – |
| … with `t_event` from `createdAt` / from `indexedAt` | – |
| … `reindexed` | – |
| … `is_clean` | – |
| … `unrelated_candidate` | – |
| posts outside the period | – |
| corrupt raw lines | – |
| Kafka messages in `bsky` | – |

## 9. Known data issues and limitations

- **`createdAt` is set by the client** and can be wrong by years (e.g. imported old posts dated
  2010). These posts get their time from `indexedAt` and are marked with `time_source`.
- **Some posts were indexed again long after they were written** (`reindexed`). They keep their
  original time but are not counted as clean.
- **`author_created` is not reliable** (negative account ages occur); it is not used as a feature.
- **Unrelated posts:** "quake" also matches the video game. Such posts are flagged, not removed.
- **Keyword not in the text:** the search service matches words in its own way, so some returned
  posts have an empty `matched_keywords`.
- **No total-volume baseline:** the search returns only posts that match the keywords, so there is
  no count of all Bluesky posts per minute. Burst detection has to compare keyword posts with their
  own recent level.
- **Likes and reposts** are counts at download time, not at the time of the earthquake.

## 10. Operational notes

- Kafka: NiFi publishes in transactions, and the commit markers also take offsets. Count messages
  with a `read_committed` consumer; summing offsets gives a higher number.
- The Spark containers run Python 3.8, so code used by Spark jobs must not use newer syntax.
- Spark does not read `conf/hadoop/hdfs-site.xml`; replication 1 is set in `conf/spark/spark-defaults.conf`.
- In Hive, `date` is a reserved word and needs backticks.
- `scripts/hive_bsky.py` sends SQL to beeline through stdin, because a PowerShell pipe adds a
  byte-order mark that breaks the first statement.
- A Spark run on the local copy (`--input file:///...`) should use a separate `--summary-file`,
  otherwise `hive_bsky.py verify` compares Hive with the wrong numbers.

## 11. Test cases

F = functional (automated, `pytest`), I = integration (pipeline run), Q = data quality.
Automated tests run on every pull request (GitHub Actions) and locally with
`python scripts/run_spark_tests.py`.

| ID | Objective | Steps | Expected result | Actual result |
|---|---|---|---|---|
| BSKY-F-01 | Login works and search goes through the AppView proxy | `test_login_sets_bearer_token_and_search_uses_proxy_header` | token set; proxy header sent with search only | – |
| BSKY-F-02 | Errors that retrying cannot fix stop the client | `test_login_failure_is_fatal`, `test_403_is_fatal_without_retry` | FatalError, no repeated requests | – |
| BSKY-F-03 | Temporary errors are retried, but not forever | `test_401_triggers_relogin_and_retry`, `test_429_waits_retry_after_then_retries`, `test_5xx_retries_with_backoff`, `test_retries_are_bounded` | re-login on 401, waits on 429/5xx, gives up after 8 attempts | – |
| BSKY-F-04 | Client slows down before hitting the rate limit | `test_low_remaining_rate_limit_backs_off` | waits until the limit resets | – |
| BSKY-F-05 | All result pages are read; a full window is detected | `test_fetch_follows_cursor_and_sends_window`, `test_fetch_reports_capped_when_page_limit_reached` | pages followed by cursor; `capped` when the page limit is reached | – |
| BSKY-F-06 | Only the agreed fields are stored, no profile data | `test_slim_keeps_schema_and_drops_profile_data` | exactly the 10 fields of section 4 | – |
| BSKY-F-07 | Busy windows are split, halves before parent | `test_window_is_split_until_it_fits` | two 30-min halves, parent marked done last | – |
| BSKY-F-08 | A new run downloads only what is missing | `test_finished_windows_are_skipped_on_resume`, `test_window_covered_by_smaller_finished_windows_is_not_fetched`, `test_only_the_half_with_a_gap_is_fetched`, `test_pending_windows_count_coverage_not_exact_keys` | covered windows skipped, also with a different window size | – |
| BSKY-F-09 | Download survives network errors | `test_backfill_retries_window_after_network_error` | waits and retries the window | – |
| BSKY-F-10 | Raw reading ignores other files and keeps broken lines visible | `test_read_raw_ignores_other_files_and_counts_corrupt_lines`, `test_read_raw_finds_files_in_hdfs_style_partition_dirs` | only `*.jsonl` read; corrupt line counted | – |
| BSKY-F-11 | Duplicates are removed by `uri` | `test_duplicates_are_removed_by_uri_and_earliest_file_wins` | one row per post, `n_copies` correct | – |
| BSKY-F-12 | Event time rule at its boundaries | `test_t_event_rule`, `test_in_period_uses_t_event` | 5 min tolerance exact; bad dates fall back to `indexedAt` | – |
| BSKY-F-13 | `reindexed` and `is_clean` thresholds | `test_reindexed_threshold_is_strictly_more_than_one_day`, `test_clean_lag_window_is_minus5_to_plus10_minutes` | > 1 day; −5…+10 min inclusive | – |
| BSKY-F-14 | Keyword columns; exclusions only flag | `test_unrelated_candidate_is_a_flag_only`, `test_matched_keywords`, `test_keyword_config_loads` | flags correct, no rows removed | – |
| BSKY-F-15 | Per-minute table and control numbers | `test_minute_counts_are_dense_and_count_authors`, `test_summary_control_numbers` | every minute present; counts correct | – |
| BSKY-I-01 | Docker stack is healthy | `python scripts/smoke_test.py` | all services OK | – |
| BSKY-I-02 | NiFi loads every file into HDFS unchanged | run the flow, then `python scripts/verify_bsky_ingest.py` (HDFS checks) | same files and sizes as `data/raw/bsky`, in `date=` folders | – |
| BSKY-I-03 | NiFi publishes every post to Kafka | `python scripts/verify_bsky_ingest.py` (Kafka checks: `read_committed` count, JSON parse) | messages = local lines; 0 invalid | – |
| BSKY-I-04 | Spark job processes the HDFS data | run the job (section 7, step 3) | finishes; summary file written | – |
| BSKY-I-05 | Hive shows the same data as Spark | `python scripts/hive_bsky.py verify` | 8/8 checks OK | – |
| BSKY-I-06 | All tests pass in CI | GitHub Actions run on the pull request | all passed, none skipped | – |
| BSKY-Q-01 | Raw files are readable | `corrupt_rows` in the summary | 0 | – |
| BSKY-Q-02 | Every day of the period was downloaded | `--dry-run`; `raw_files` in the summary | 0 pending windows; 731 daily files | – |
| BSKY-Q-03 | Posts are unique after cleaning | Hive: `count(DISTINCT uri)` = `count(*)` on `bsky_posts` | equal | – |
| BSKY-Q-04 | Per-minute table is complete and consistent | Hive: rows of `bsky_minute`; sum of `n_posts` | 1 052 640 rows; sum = posts in period | – |
| BSKY-Q-05 | Server timestamps are always valid | `unparsable_indexed_at` in the summary | 0 | – |
