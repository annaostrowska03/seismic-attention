# Bluesky – test evidence

Outputs of the checks behind the "Actual result" column in [docs/bluesky.md](../../bluesky.md), section 11
(test IDs as in the MS2 test documentation, see [docs/testing.md](../../testing.md)).
Each log starts with the test cases it covers, the command, the run time and the exit code.

| File | Test cases |
|---|---|
| `01_smoke_test.txt` | ENV-I-01 (Docker stack healthy) |
| `02_pytest_spark_container.txt` | BSKY-F-01 … BSKY-F-16 (run of 22.11, 75 tests in the repository at that time; current run of all 82 tests: [../pytest_all.txt](../pytest_all.txt)) |
| `03_backfill_dry_run.txt` | BSKY-Q-02 |
| `04_verify_bsky_ingest.txt` | BSKY-I-01, BSKY-I-02 |
| `05_hive_bsky_verify.txt` | BSKY-I-04, BSKY-Q-03, BSKY-Q-04 |
| `06_spark_summary.txt` | BSKY-I-03, BSKY-Q-01, BSKY-Q-05 |
| `ci_run.png` | CI-02 (GitHub Actions run of the pull request) |
| `nifi_flow.png` | NiFi process group `bluesky-ingest` on the canvas |
