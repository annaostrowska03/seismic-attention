# Bluesky – test evidence

Outputs of the checks behind the "Actual result" column in [docs/bluesky.md](../../bluesky.md), section 11.
Each log starts with the test cases it covers, the command, the run time and the exit code.

| File | Test cases |
|---|---|
| `01_smoke_test.txt` | BSKY-I-01 |
| `02_pytest_spark_container.txt` | BSKY-F-01 … BSKY-F-16 (all automated tests, run in the Spark container) |
| `03_backfill_dry_run.txt` | BSKY-Q-02 |
| `04_verify_bsky_ingest.txt` | BSKY-I-02, BSKY-I-03 |
| `05_hive_bsky_verify.txt` | BSKY-I-05, BSKY-Q-03, BSKY-Q-04 |
| `06_spark_summary.txt` | BSKY-I-04, BSKY-Q-01, BSKY-Q-05 |
| `ci_run.png` | BSKY-I-06 (GitHub Actions run of the pull request) |
| `nifi_flow.png` | NiFi process group `bluesky-ingest` on the canvas |
