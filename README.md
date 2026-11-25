# seismic-attention

**Internet as an Earthquake Sensor (USGS + Bluesky + GDELT)**
Big Data Analytics project, Team MANG.

For every M4.5+ earthquake in the USGS catalogue, the system measures the Bluesky reaction
(human posts separated from bots) and predicts whether the event will be covered by global media
(GDELT, 48 h), at T0 (USGS features only) and at T+15 min (with social-reaction features).

The system works on historical data from 2024 and 2025 (Lambda architecture: NiFi, Kafka, HDFS, Spark,
Hive). In MS2 the streaming part is shown by reading the Bluesky history from Kafka with Spark Structured
Streaming; a simulated run that replays the whole period in time order, as if it were arriving live, is
planned for MS3/MS4 ([docs/architecture.md](docs/architecture.md)).

## Status (MS2)

Done: acquisition and pre-processing of all three sources (NiFi flows, history backfill, Spark jobs, Hive
tables), coverage labels, exploratory analysis, Bluesky stream processing, 82 automated tests and the data
checks ([docs/testing.md](docs/testing.md)).
Planned: bot filter, burst detection, coverage models, serving layer in Apache Cassandra, FastAPI,
Streamlit dashboard, simulated run orchestrated by Apache Airflow.

## Infrastructure (Docker)

```bash
docker compose up -d --build
python scripts/smoke_test.py
```

Full guide: [docs/SETUP.md](docs/SETUP.md).

## Python setup

Python 3.11+ on the host; the Spark containers use their own Python 3.8 ([docs/SETUP.md](docs/SETUP.md#python-versions)).

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
source .venv/bin/activate       # macOS / Linux
pip install -r requirements.txt
pytest
```

The Spark tests need Java and are skipped without it. To run all tests inside the Spark
container (no local Java needed):

```bash
python scripts/run_spark_tests.py
```

GitHub Actions runs the full test suite on every pull request and push to `main`
(`.github/workflows/tests.yml`) and fails if any test is skipped.

## Documentation

- [docs/SETUP.md](docs/SETUP.md) – Docker stack setup, Python versions
- [docs/architecture.md](docs/architecture.md) – Lambda architecture, data model, planned serving layer and simulated run
- [docs/hdfs_layout.md](docs/hdfs_layout.md) – HDFS zones and Hive tables
- [docs/usgs.md](docs/usgs.md) – USGS source: live NiFi flow, history, features
- [docs/gdelt.md](docs/gdelt.md) – GDELT source, live NiFi flow and media-coverage labels
- [docs/bluesky.md](docs/bluesky.md) – Bluesky source: acquisition, processing rules, Hive tables
- [docs/streaming.md](docs/streaming.md) – Spark Structured Streaming: implementation, execution and validation
- [docs/testing.md](docs/testing.md) – test types, how to run, all test IDs; logs in [docs/test_evidence/](docs/test_evidence/README.md)
- [nifi/README.md](nifi/README.md) – NiFi flows

## Pipeline in short

```bash
# history (to data/raw/, not in Git)
python scripts/backfill_usgs.py                  # USGS catalogue, study period from config.py
python scripts/backfill_gdelt.py                 # GDELT GKG files, earthquake articles only
python scripts/bsky_backfill.py                  # Bluesky search API (needs .env, see docs/bluesky.md)

# NiFi flows (Bluesky files -> HDFS + Kafka; live USGS and GDELT flows: production only, not used in the PoC)
python scripts/nifi_gdelt_flow.py create
python scripts/nifi_gdelt_flow.py start
python scripts/nifi_bsky_flow.py create
python scripts/nifi_bsky_flow.py start
# USGS: import nifi/flows/usgs.json in the NiFi UI (nifi/README.md)

# Spark jobs (inside the Spark container)
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/load_history.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_coverage_labels.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/eda_history.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_bsky_preprocess.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_bsky_streaming.py

# Hive tables over the raw zone (each script also has "verify")
python scripts/hive_usgs.py create
python scripts/hive_gdelt.py create
python scripts/hive_bsky.py create
```

In Git Bash, prefix the `docker compose exec` commands with `MSYS_NO_PATHCONV=1`.

## Repository layout

| Path | Content |
|---|---|
| `docker-compose.yml`, `docker/`, `conf/` | Docker stack and its configuration |
| `nifi/flows/` | exported NiFi flows (USGS, GDELT, Bluesky) |
| `hive/` | Hive DDL of the raw-zone and curated tables |
| `src/seismic_attention/` | shared code: config, Bluesky client and backfill, pre-processing, features, labels, streaming |
| `scripts/` | backfill, NiFi flow builders, Spark jobs, Hive helpers, verification scripts |
| `tests/` | pytest tests (82) |
| `data/sample/` | small Bluesky sample with the edge cases; the full data are not in Git |
| `docs/` | documentation and test evidence |

## Team

Anna Ostrowska, Gabriela Majstrak, Norbert Frydrysiak, Michał Kukla
