# Test evidence

Logs and screenshots behind the "Actual result" column of the MS2 test documentation. Test types, test
IDs and how to run everything: [docs/testing.md](../testing.md).

| Path | Content |
|---|---|
| [pytest_all.txt](pytest_all.txt) | all 82 automated tests, run in the Spark container (Python 3.8), 2026-11-25 |
| [bluesky/](bluesky/README.md) | Bluesky source: smoke test, backfill, NiFi ingestion, Spark job, Hive checks, CI run, NiFi canvas |
| [team/](team/README.md) | history (USGS, GDELT), coverage labels, streaming vs batch |

Each log starts with the test cases it covers, the command, the run time and the exit code.
