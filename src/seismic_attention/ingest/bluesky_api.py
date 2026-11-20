"""Minimal client for Bluesky post search (app.bsky.feed.searchPosts).

Search works only through the user's own PDS with the ``atproto-proxy`` header
(the public AppView endpoint returned 403). Authentication uses an app password.

Rate limits (per account / IP):
- createSession: 30 per 5 min and 300 per day, so we log in once and refresh only on 401.
- general: about 3000 requests per 5 min per IP; we keep MIN_INTERVAL between requests and
  back off when ``ratelimit-remaining`` gets low.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

PDS_URL = "https://bsky.social/xrpc"
SESSION_URL = f"{PDS_URL}/com.atproto.server.createSession"
SEARCH_URL = f"{PDS_URL}/app.bsky.feed.searchPosts"
APPVIEW_PROXY = "did:web:api.bsky.app#bsky_appview"
USER_AGENT = "seismic-attention-student-project"

PAGE_LIMIT = 100            # posts per page (API maximum)
MAX_PAGES = 100             # 100 pages x 100 posts; beyond that the window is split
MIN_INTERVAL = 0.25         # seconds between requests (~4 req/s)
MAX_ATTEMPTS = 8            # attempts per single request (401 / 429 / 5xx)
LOW_REMAINING = 50          # back off when fewer requests than this remain


class FatalError(Exception):
    """Error that retrying cannot fix (bad credentials, HTTP 400/403/404)."""


class RetryExhaustedError(RuntimeError):
    """A single request kept failing with retryable errors."""


def iso(dt: datetime) -> str:
    """UTC timestamp in the format used by the API and by resume keys (no fractions)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class BlueskyClient:
    def __init__(
        self,
        handle: str,
        app_password: str,
        session: Optional[requests.Session] = None,
        min_interval: float = MIN_INTERVAL,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.handle = handle
        self.app_password = app_password
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.min_interval = min_interval
        self.sleep = sleep
        self.clock = clock
        self.last_request = 0.0
        self.n_requests = 0
        self.login()

    def login(self) -> None:
        r = self.session.post(
            SESSION_URL,
            json={"identifier": self.handle, "password": self.app_password},
            timeout=60,
        )
        if r.status_code != 200:
            raise FatalError(f"login failed: HTTP {r.status_code} {r.text[:150]}")
        self.session.headers["Authorization"] = f"Bearer {r.json()['accessJwt']}"

    def _respect_rate_limit(self, r: requests.Response) -> None:
        remaining = r.headers.get("ratelimit-remaining")
        reset = r.headers.get("ratelimit-reset")
        if remaining is None or reset is None:
            return
        try:
            if int(remaining) < LOW_REMAINING:
                value = float(reset)
                # the header is either an epoch timestamp or a number of seconds
                secs = value - self.clock() if value > 1e9 else value
                self.sleep(max(1.0, min(secs, 300.0)) + 1)
        except ValueError:
            pass

    def search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """One searchPosts request with retries on 401, 429 and 5xx."""
        for attempt in range(MAX_ATTEMPTS):
            wait = self.min_interval - (self.clock() - self.last_request)
            if wait > 0:
                self.sleep(wait)
            self.last_request = self.clock()
            r = self.session.get(
                SEARCH_URL, params=params, headers={"atproto-proxy": APPVIEW_PROXY}, timeout=60
            )
            self.n_requests += 1
            self._respect_rate_limit(r)
            if r.status_code == 200:
                return r.json()
            # an expired access token can also come back as HTTP 400 with error "ExpiredToken"
            if r.status_code == 401 or "ExpiredToken" in r.text:
                self.login()
                continue
            if r.status_code == 429:
                self.sleep(int(r.headers.get("retry-after", 60)))
                continue
            if r.status_code >= 500:
                self.sleep(5 * (attempt + 1))
                continue
            raise FatalError(f"HTTP {r.status_code}: {r.text[:200]}")
        raise RetryExhaustedError(f"too many attempts for one request: {params}")


def fetch(
    client: BlueskyClient,
    keyword: str,
    start: datetime,
    end: datetime,
    max_pages: int = MAX_PAGES,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Fetch all posts for a keyword in [start, end). Returns (posts, capped_by_page_limit)."""
    posts: List[Dict[str, Any]] = []
    cursor = None
    for _ in range(max_pages):
        params = {"q": keyword, "limit": PAGE_LIMIT, "sort": "latest",
                  "since": iso(start), "until": iso(end)}
        if cursor:
            params["cursor"] = cursor
        page = client.search(params)
        posts += page.get("posts", [])
        cursor = page.get("cursor")
        if not cursor:
            return posts, False
    return posts, True


def slim(post: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only the fields needed for analysis (no handle, no profile data)."""
    record, author = post.get("record", {}), post.get("author", {})
    return {
        "uri": post["uri"],
        "createdAt": record.get("createdAt"),
        "indexedAt": post.get("indexedAt"),
        "text": record.get("text"),
        "langs": record.get("langs"),
        "did": author.get("did"),
        "author_created": author.get("createdAt"),
        "reply": "reply" in record,
        "likes": post.get("likeCount"),
        "reposts": post.get("repostCount"),
    }
