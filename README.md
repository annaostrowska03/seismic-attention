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

## Team

Anna Ostrowska, Gabriela Majstrak, Norbert Frydrysiak, Michał Kukla
