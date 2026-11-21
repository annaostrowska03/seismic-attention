# seismic-attention

**Internet as an Earthquake Sensor (USGS + Bluesky + GDELT)** 
Big Data Analytics project, Team MANG.

For every M4.5+ earthquake in the USGS catalogue, the system measures the Bluesky reaction
(human posts separated from bots) and predicts whether the event will be covered by global media
(GDELT, 48 h), at T0 (USGS features only) and at T+15 min (with social-reaction features).

The system works on historical data from 2024 and 2025. The streaming part is shown by replaying
these data in time order through NiFi, Kafka and Spark, as if they were arriving live.

## Infrastructure (Docker)

```bash
docker compose up -d --build
python scripts/smoke_test.py
```

Full guide: [docs/SETUP.md](docs/SETUP.md).

## Python setup

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

GitHub Actions runs the full test suite on every pull request (`.github/workflows/tests.yml`).

## Documentation

- [docs/SETUP.md](docs/SETUP.md) – Docker stack setup
- [docs/bluesky.md](docs/bluesky.md) – Bluesky source: acquisition, processing rules, Hive tables, test cases

## History

USGS catalogue (M4.5+) and GDELT articles with an earthquake theme, downloaded to `data/raw/`:

```bash
python scripts/backfill_usgs.py                                         # study period from config.py
python scripts/backfill_gdelt.py --start 2025-03-01 --end 2025-04-01     # a shorter range, end excluded
```

Load to HDFS (Parquet) and Hive tables `seismic.usgs_history`, `seismic.gdelt_history`,
`seismic.gdelt_files` (status of each 15-min GDELT file), then run the analysis:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/load_history.py
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/eda_history.py
```

The Spark containers run Python 3.8, so `src/seismic_attention/features.py` must work on 3.8.

## Files

- `nifi/flows/usgs.json` - NiFi flow: USGS feed every minute to Kafka `usgs` and HDFS `/raw/usgs`
- `scripts/backfill_usgs.py` - downloads the USGS catalogue month by month
- `scripts/backfill_gdelt.py` - downloads GDELT GKG files and keeps earthquake articles
- `scripts/load_history.py` - loads the downloaded history to HDFS and Hive
- `scripts/eda_history.py` - exploratory analysis: media coverage of earthquakes on the history
- `src/seismic_attention/features.py` - USGS event features
- `tests/` - pytest tests

## Team

Anna Ostrowska, Gabriela Majstrak, Norbert Frydrysiak, Michał Kukla
