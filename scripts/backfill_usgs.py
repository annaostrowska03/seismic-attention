import json
import sys
from pathlib import Path

import requests

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
OUT_DIR = Path("data/raw/usgs_history")
MIN_MAGNITUDE = 4.5


def months(first, last):
    year, month = map(int, first.split("-"))
    last_year, last_month = map(int, last.split("-"))
    result = []
    while (year, month) <= (last_year, last_month):
        result.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return result


def fetch_month(year, month):
    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    params = {
        "format": "geojson",
        "starttime": f"{year}-{month:02d}-01",
        "endtime": f"{next_year}-{next_month:02d}-01",
        "minmagnitude": MIN_MAGNITUDE,
        "orderby": "time-asc",
    }
    response = requests.get(URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()["features"]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for year, month in months(sys.argv[1], sys.argv[2]):
        features = fetch_month(year, month)
        with open(OUT_DIR / f"{year}-{month:02d}.jsonl", "w", encoding="utf-8") as f:
            for feature in features:
                f.write(json.dumps(feature) + "\n")
        print(f"{year}-{month:02d}: {len(features)} events")


if __name__ == "__main__":
    main()
