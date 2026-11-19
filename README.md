# seismic-attention

**Internet as an Earthquake Sensor (USGS + Bluesky + GDELT)** 
Big Data Analytics project, Team MANG.

For every new M4.5+ earthquake in the USGS catalogue, the system measures the Bluesky reaction
(human posts separated from bots) and predicts whether the event will be covered by global media
(GDELT, 48 h), at T0 (USGS features only) and at T+15 min (with social-reaction features).

## Infrastructure (Docker)

```bash
docker compose up -d --build
python scripts/smoke_test.py
```

Full guide: [docs/SETUP.md](docs/SETUP.md).

## Python setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
pytest
```

## History

USGS catalogue (M4.5+) and GDELT articles with an earthquake theme, downloaded to `data/raw/`:

```bash
python scripts/backfill_usgs.py 2024-01 2026-11 # first month 2024-01, last month 2026-11
python scripts/backfill_gdelt.py 2024-01-01 2026-11-01   # first day, last day; end date excluded
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
