"""Resumable backfill of Bluesky posts into daily JSONL files.

For every keyword, posts are fetched window by window. A window whose results hit the page
limit is split in half recursively (down to MIN_WINDOW).

Resume: finished windows are appended to ``_done.txt`` as ``keyword|start|end``. A window is
skipped when finished windows of the same keyword cover it, whatever their size, so a different
window size or date range fetches only what is missing. A parent window is marked done only
after both halves are done. Adding a new keyword fetches only that keyword. The same post can
match several keywords, so raw files contain duplicates; they are removed later by ``uri``.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Set, Tuple

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

Span = Tuple[datetime, datetime]


def window_key(keyword: str, start: datetime, end: datetime) -> str:
    return f"{keyword}|{iso(start)}|{iso(end)}"


def parse_key(key: str) -> Optional[Tuple[str, datetime, datetime]]:
    """Inverse of window_key; None for a blank or malformed line."""
    try:
        keyword, start, end = key.rsplit("|", 2)
        return keyword, _from_iso(start), _from_iso(end)
    except ValueError:
        return None


def _from_iso(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _merge(spans: Iterable[Span]) -> List[Span]:
    """Sort intervals and join overlapping or touching ones: (0,2) (1,3) (3,4) -> (0,4)."""
    merged: List[Span] = []
    for a, b in sorted(spans):
        if merged and a <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    return merged


class DoneLog:
    """Finished windows from the append-only ``_done.txt``, with a coverage check per keyword."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.keys: Set[str] = set()
        self._spans: Dict[str, List[Span]] = {}
        if path.exists():
            self.keys = set(path.read_text(encoding="utf-8").splitlines())
            by_keyword: Dict[str, List[Span]] = {}
            for parsed in filter(None, map(parse_key, self.keys)):
                keyword, start, end = parsed
                by_keyword.setdefault(keyword, []).append((start, end))
            self._spans = {kw: _merge(spans) for kw, spans in by_keyword.items()}

    def __contains__(self, key: str) -> bool:
        return key in self.keys

    def covers(self, keyword: str, start: datetime, end: datetime) -> bool:
        """True if finished windows of ``keyword`` cover [start, end) without a gap."""
        return any(a <= start and end <= b for a, b in self._spans.get(keyword, ()))

    def add(self, key: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(key + "\n")
        self.keys.add(key)
        parsed = parse_key(key)
        if parsed:
            keyword, start, end = parsed
            self._spans[keyword] = _merge(self._spans.get(keyword, []) + [(start, end)])


def iter_windows(start: datetime, end: datetime, step: timedelta) -> Iterator[Tuple[datetime, datetime]]:
    """Consecutive [a, b) windows of ``step`` from ``start``; the last one may be shorter."""
    a = start
    while a < end:
        b = min(a + step, end)
        yield a, b
        a = b


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
    if done.covers(keyword, start, end):
        return
    key = window_key(keyword, start, end)
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


def pending_windows(
    keywords: Iterable[str],
    start: datetime,
    end: datetime,
    step: timedelta,
    raw_dir: Path,
) -> Dict[str, int]:
    """Number of top-level windows not yet covered by finished windows, per keyword (no API calls)."""
    done = DoneLog(raw_dir / DONE_FILE_NAME)
    windows = list(iter_windows(start, end, step))
    return {kw: sum(not done.covers(kw, a, b) for a, b in windows) for kw in keywords}


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
    for a, b in iter_windows(start, end, step):
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
