"""Backfill Bluesky posts (app.bsky.feed.searchPosts) into data/raw/bsky/YYYY-MM-DD.jsonl.

Usage (from the repo root):
    python scripts/bsky_backfill.py --dry-run     # what is still missing in the study period
    python scripts/bsky_backfill.py               # fetch only what is missing
    python scripts/bsky_backfill.py --start 2025-03-01 --end 2025-04-01   # a shorter range

Defaults: the study period from src/seismic_attention/config.py (2024 + 2025), 6-hour windows.
Requires BSKY_HANDLE and BSKY_APP_PASSWORD in .env (never commit it).
Resumable: finished windows are listed in data/raw/bsky/_done.txt; a window already covered by
finished windows (of any size) is skipped. To fetch everything again, move data/raw/bsky away.
Keywords: src/seismic_attention/ingest/bluesky_keywords.json.
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))  # run without installing the package

from seismic_attention.config import PERIOD_END, PERIOD_START  # noqa: E402
from seismic_attention.ingest.bluesky_api import BlueskyClient  # noqa: E402
from seismic_attention.ingest.bluesky_backfill import pending_windows, run_backfill  # noqa: E402
from seismic_attention.ingest.keywords import load_keywords  # noqa: E402

RAW_DIR = ROOT / "data" / "raw" / "bsky"


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default=PERIOD_START,
                    help="YYYY-MM-DD (UTC, inclusive), default %(default)s")
    ap.add_argument("--end", default=PERIOD_END,
                    help="YYYY-MM-DD (UTC, exclusive), default %(default)s")
    ap.add_argument("--window-hours", type=int, default=6,
                    help="top-level window size; busy windows are split automatically")
    ap.add_argument("--dry-run", action="store_true",
                    help="only print how many windows per keyword are still to fetch")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    step = timedelta(hours=args.window_hours)
    keywords = load_keywords().search_keywords

    logging.info("period %s - %s, %d keywords, %d h windows",
                 args.start, args.end, len(keywords), args.window_hours)
    pending = pending_windows(keywords, start, end, step, RAW_DIR)
    for kw, n in pending.items():
        logging.info("pending windows: %-16s %d", kw, n)
    if args.dry_run:
        return

    # utf-8-sig: a .env saved by Notepad or PowerShell may start with a BOM, which would
    # otherwise end up in the first variable name
    load_dotenv(ROOT / ".env", override=True, encoding="utf-8-sig")
    client = BlueskyClient(os.environ["BSKY_HANDLE"], os.environ["BSKY_APP_PASSWORD"])
    t0 = time.time()
    skipped = run_backfill(client, keywords, start, end, step, RAW_DIR, sleep=time.sleep)
    logging.info("finished in %.0f s, requests: %d", time.time() - t0, client.n_requests)
    if skipped:
        logging.warning("%d window(s) skipped after server errors, a later run retries them: %s",
                        len(skipped), ", ".join(skipped))


if __name__ == "__main__":
    main()
