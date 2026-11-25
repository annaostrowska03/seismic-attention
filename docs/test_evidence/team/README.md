# USGS, GDELT, history, labels, streaming – test evidence

Outputs behind the "Actual result" column of the MS2 test documentation (test IDs and the full list:
[docs/testing.md](../../testing.md)). Runs on the local Docker stack; data of the study period
2024-01-01 – 2025-12-31 unless stated otherwise. Each log starts with the test cases it covers, the
command and the run time.

| File | Test cases |
|---|---|
| `01_load_history.txt` | HIST-I-03 (load the downloaded history to HDFS and Hive) |
| `02_coverage_labels.txt` | LAB-I-01 (label table built) |
| `03_hive_label_checks.txt` | LAB-Q-02 (one row per earthquake, NULL label only for incomplete GDELT windows) |
| `04_usgs_gdelt_period_stats.txt` | HIST-Q-05 (statistics of the study period) |
| `05_stream_vs_batch.txt` | STR-I-01, STR-Q-01 (streaming run and comparison with the batch table) |
| `06_verify_history.txt` | HIST-Q-01 … HIST-Q-04 (`scripts/verify_history.py`) |

All automated tests of the repository: [../pytest_all.txt](../pytest_all.txt) (82 passed).
