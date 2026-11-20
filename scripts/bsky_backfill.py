"""Backfill Bluesky posts (app.bsky.feed.searchPosts) into data/raw/bsky/YYYY-MM-DD.jsonl.

Usage (from the repo root):
    python scripts/bsky_backfill.py --start 2024-01-01 --end 2024-01-02
    python scripts/bsky_backfill.py --start 2024-01-01 --end 2025-01-01 --window-hours 6

Requires BSKY_HANDLE and BSKY_APP_PASSWORD in .env (never commit it).
Resumable: finished windows are listed in data/raw/bsky/_done.txt.
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
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (UTC, inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (UTC, exclusive)")
    ap.add_argument("--window-hours", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true",
                    help="only print how many windows per keyword are still to fetch")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    step = timedelta(hours=args.window_hours)
    keywords = load_keywords().search_keywords

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
    run_backfill(client, keywords, start, end, step, RAW_DIR, sleep=time.sleep)
    logging.info("finished in %.0f s, requests: %d", time.time() - t0, client.n_requests)


if __name__ == "__main__":
    main()
