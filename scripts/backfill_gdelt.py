import argparse
import csv
import io
import json
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from seismic_attention.config import PERIOD_END, PERIOD_START

URL = "http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip"
OUT_DIR = Path("data/raw/gdelt_history")
THEME = "EARTHQUAKE" # matches NATURAL_DISASTER_EARTHQUAKE and NATURAL_DISASTER_EARTHQUAKES
THREADS = 8


def day_stamps(day):
    """Returns a list of all files names from GDELT API from a specified day. GDELT has files with a format YYYYMMDDHHMMSS, for example 20250101000000. GDELT publishes one file per 15 minutes."""
    return [(day + timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M%S") for i in range(96)]


def parse_gkg(text):
    """Earthquake articles from the content of one GKG file (tab-separated, no header)."""
    articles = []
    for line in text.split("\n"):
        cols = line.split("\t")
        if len(cols) < 10 or THEME not in cols[7]:
            continue
        articles.append({
            "gkg_id": cols[0],
            "date": cols[1],
            "source": cols[3],
            "url": cols[4],
            "themes": cols[7],
            "locations": cols[9],
        })
    return articles


def fetch_file(stamp):
    """Returns (stamp, status, articles). Network errors are retried, then raised."""
    for attempt in range(5):
        try:
            response = requests.get(URL.format(stamp), timeout=120)
            break
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(10)
    if response.status_code == 404:
        return stamp, "missing", []
    response.raise_for_status()
    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            text = archive.read(archive.namelist()[0]).decode("utf-8", errors="replace")
    except zipfile.BadZipFile:
        return stamp, "broken", []
    return stamp, "ok", parse_gkg(text)


def backfill_day(day):
    name = day.strftime("%Y-%m-%d")
    status_path = OUT_DIR / "files" / f"{name}.csv"
    if status_path.exists():
        return
    with ThreadPoolExecutor(THREADS) as pool:
        results = list(pool.map(fetch_file, day_stamps(day)))

    with open(OUT_DIR / "articles" / f"{name}.jsonl", "w", encoding="utf-8") as f:
        for _, _, articles in results:
            for article in articles:
                f.write(json.dumps(article) + "\n")
                
    with open(status_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["file", "status", "articles"])
        for stamp, status, articles in results:
            writer.writerow([stamp, status, len(articles)])

    ok = sum(status == "ok" for _, status, _ in results)
    print(f"{name}: {ok}/96 files, {sum(len(a) for _, _, a in results)} articles", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=PERIOD_START, help="YYYY-MM-DD (UTC, inclusive), default %(default)s")
    parser.add_argument("--end", default=PERIOD_END, help="YYYY-MM-DD (UTC, exclusive), default %(default)s")
    args = parser.parse_args()
    start = datetime.fromisoformat(args.start)
    end = datetime.fromisoformat(args.end)
    (OUT_DIR / "articles").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "files").mkdir(parents=True, exist_ok=True)
    day = start
    while day < end:
        backfill_day(day)
        day += timedelta(days=1)


if __name__ == "__main__":
    main()
