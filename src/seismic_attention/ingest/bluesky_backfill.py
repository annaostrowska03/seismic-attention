"""Resumable backfill of Bluesky posts into daily JSONL files.

For every keyword, posts are fetched window by window. A window whose results hit the page
limit is split in half recursively (down to MIN_WINDOW).

Resume: finished windows are appended to ``_done.txt`` as ``keyword|start|end`` and skipped on
the next run. A parent window is marked done only after both halves are done. Adding a new
keyword fetches only that keyword. The same post can match several keywords, so raw files
contain duplicates; they are removed later by ``uri``.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Set

import requests

from seismic_attention.ingest.bluesky_api import (
    MAX_PAGES,
    BlueskyClient,
    fetch,
    iso,
    slim,
)

log = logging.getLogger(__name__)

MIN_WINDOW = timedelta(minutes=1)
RETRIES_PER_WINDOW = 12     # attempts per window after network errors / exhausted retries
WAIT_AFTER_ERROR = 300      # seconds
DONE_FILE_NAME = "_done.txt"


class DoneLog:
    """Append-only set of finished window keys, persisted in ``_done.txt``."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.keys: Set[str] = set()
        if path.exists():
            self.keys = set(path.read_text(encoding="utf-8").splitlines())

    def __contains__(self, key: str) -> bool:
        return key in self.keys

    def add(self, key: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(key + "\n")
        self.keys.add(key)


def window_key(keyword: str, start: datetime, end: datetime) -> str:
    return f"{keyword}|{iso(start)}|{iso(end)}"


def process_window(
    client: BlueskyClient,
    keyword: str,
    start: datetime,
    end: datetime,
    done: DoneLog,
    raw_dir: Path,
    max_pages: int = MAX_PAGES,
) -> None:
    """Fetch one window (splitting it if needed) and append the posts to the daily file."""
    key = window_key(keyword, start, end)
    if key in done:
        return
    posts, capped = fetch(client, keyword, start, end, max_pages=max_pages)
    if capped and (end - start) > MIN_WINDOW:
        mid = start + (end - start) / 2
        process_window(client, keyword, start, mid, done, raw_dir, max_pages)
        process_window(client, keyword, mid, end, done, raw_dir, max_pages)
        done.add(key)               # parent is done because both halves are done
        return
    if capped:
        log.warning("window truncated by page limit: %s", key)
    raw_dir.mkdir(parents=True, exist_ok=True)
    with open(raw_dir / f"{start:%Y-%m-%d}.jsonl", "a", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(slim(post), ensure_ascii=False) + "\n")
    done.add(key)
    log.info("%s -> %d", key, len(posts))


def run_backfill(
    client: BlueskyClient,
    keywords: Iterable[str],
    start: datetime,
    end: datetime,
    step: timedelta,
    raw_dir: Path,
    sleep: Callable[[float], None],
    retries: int = RETRIES_PER_WINDOW,
    wait_after_error: float = WAIT_AFTER_ERROR,
) -> None:
    """Backfill [start, end) for all keywords in windows of ``step``."""
    keywords = list(keywords)
    done = DoneLog(raw_dir / DONE_FILE_NAME)
    a = start
    while a < end:
        b = min(a + step, end)
        for kw in keywords:
            for _ in range(retries):
                try:
                    process_window(client, kw, a, b, done, raw_dir)
                    break
                except (requests.RequestException, RuntimeError) as exc:
                    log.error("error (%s, %s): %r - waiting %s s", kw, iso(a), exc,
                              wait_after_error)
                    sleep(wait_after_error)
            else:
                raise SystemExit(f"window {kw} {iso(a)} failed after {retries} attempts")
        a = b
