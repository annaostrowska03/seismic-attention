import argparse
import json
import sys
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from seismic_attention.config import PERIOD_END, PERIOD_START

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
OUT_DIR = Path("data/raw/usgs_history")
MIN_MAGNITUDE = 4.5


def month_ranges(start, end):
    """Splits [start, end) into pieces that do not cross a month boundary."""
    ranges = []
    while start < end:
        if start.month == 12:
            next_month = date(start.year + 1, 1, 1)
        else:
            next_month = date(start.year, start.month + 1, 1)
        ranges.append((start, min(next_month, end)))
        start = next_month
    return ranges


def fetch(start, end):
    params = {
        "format": "geojson",
        "starttime": start.isoformat(),
        "endtime": end.isoformat(),
        "minmagnitude": MIN_MAGNITUDE,
        "orderby": "time-asc",
    }
    response = requests.get(URL, params=params, timeout=120)
    response.raise_for_status()
    return response.json()["features"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=PERIOD_START, help="YYYY-MM-DD (UTC, inclusive), default %(default)s")
    parser.add_argument("--end", default=PERIOD_END, help="YYYY-MM-DD (UTC, exclusive), default %(default)s")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for start, end in month_ranges(date.fromisoformat(args.start), date.fromisoformat(args.end)):
        features = fetch(start, end)
        name = f"{start:%Y-%m}"
        with open(OUT_DIR / f"{name}.jsonl", "w", encoding="utf-8") as f:
            for feature in features:
                f.write(json.dumps(feature) + "\n")
        print(f"{name}: {len(features)} events")


if __name__ == "__main__":
    main()
