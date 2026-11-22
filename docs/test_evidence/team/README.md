# USGS, GDELT, labels, streaming – test evidence

Outputs behind the "Actual result" column in [docs/ms2/tests_draft.md](../../ms2/tests_draft.md).
All runs on 2026-11-22 on the local Docker stack, data of the study period 2024-01-01 – 2025-12-31.
The automated tests of all modules are in [../bluesky/02_pytest_spark_container.txt](../bluesky/02_pytest_spark_container.txt) (75 passed).

| File | Test cases |
|---|---|
| `01_load_history.txt` | USGS-I-01, GDELT-I-01 |
| `02_coverage_labels.txt` | LBL-I-01 |
| `03_hive_label_checks.txt` | LBL-Q-01, LBL-Q-02 |
| `04_usgs_gdelt_period_stats.txt` | USGS-Q-01, GDELT-Q-01, GDELT-Q-02 |
| `05_stream_vs_batch.txt` | STR-I-01 |
