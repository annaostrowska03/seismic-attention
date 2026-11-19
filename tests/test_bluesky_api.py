"""Functional tests of the Bluesky acquisition module (no network: HTTP is faked)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from seismic_attention.ingest import bluesky_api as api
from seismic_attention.ingest.bluesky_backfill import (
    DONE_FILE_NAME,
    DoneLog,
    process_window,
    run_backfill,
    window_key,
)
from seismic_attention.ingest.keywords import load_keywords

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, status_code=200, data=None, headers=None, text=""):
        self.status_code = status_code
        self._data = data or {}
        self.headers = headers or {}
        self.text = text or json.dumps(self._data)

    def json(self):
        return self._data


class FakeSession:
    """Plays back scripted responses and records every request."""

    def __init__(self, get_responses=None, post_responses=None):
        self.headers = {}
        self.get_responses = list(get_responses or [])
        self.post_responses = list(post_responses or [])
        self.gets, self.posts = [], []

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if self.post_responses:
            return self.post_responses.pop(0)
        return FakeResponse(200, {"accessJwt": f"token{len(self.posts)}"})

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return self.get_responses.pop(0)


def make_client(session, sleeps=None):
    sleeps = sleeps if sleeps is not None else []
    return api.BlueskyClient("user.bsky.social", "app-pass", session=session,
                             min_interval=0, sleep=sleeps.append, clock=lambda: 0.0)


def raw_post(uri="at://did:plc:abc/app.bsky.feed.post/1", **extra):
    post = {
        "uri": uri,
        "indexedAt": "2024-01-01T07:12:00.000Z",
        "likeCount": 3,
        "repostCount": 1,
        "record": {"createdAt": "2024-01-01T07:11:30Z", "text": "Earthquake!", "langs": ["en"]},
        "author": {"did": "did:plc:abc", "handle": "someone.bsky.social",
                   "displayName": "Someone", "createdAt": "2023-05-01T00:00:00Z"},
    }
    post.update(extra)
    return post


# ---------- client ----------

def test_login_sets_bearer_token_and_search_uses_proxy_header():
    session = FakeSession(get_responses=[FakeResponse(200, {"posts": []})])
    client = make_client(session)
    client.search({"q": "earthquake"})
    assert session.headers["Authorization"] == "Bearer token1"
    url, kwargs = session.gets[0]
    assert url == api.SEARCH_URL
    assert kwargs["headers"]["atproto-proxy"] == api.APPVIEW_PROXY
    # the proxy header must not be sent to createSession (it is a PDS endpoint)
    assert "atproto-proxy" not in session.headers


def test_login_failure_is_fatal():
    session = FakeSession(post_responses=[FakeResponse(401, text="AuthenticationRequired")])
    with pytest.raises(api.FatalError):
        make_client(session)


def test_401_triggers_relogin_and_retry():
    session = FakeSession(get_responses=[FakeResponse(401, text="ExpiredToken"),
                                         FakeResponse(200, {"posts": [raw_post()]})])
    client = make_client(session)
    assert len(client.search({"q": "x"})["posts"]) == 1
    assert len(session.posts) == 2                     # initial login + re-login
    assert session.headers["Authorization"] == "Bearer token2"


def test_429_waits_retry_after_then_retries():
    sleeps = []
    session = FakeSession(get_responses=[FakeResponse(429, headers={"retry-after": "7"}),
                                         FakeResponse(200, {"posts": []})])
    make_client(session, sleeps).search({"q": "x"})
    assert 7 in sleeps


def test_5xx_retries_with_backoff():
    sleeps = []
    session = FakeSession(get_responses=[FakeResponse(502), FakeResponse(200, {"posts": []})])
    make_client(session, sleeps).search({"q": "x"})
    assert 5 in sleeps


def test_403_is_fatal_without_retry():
    session = FakeSession(get_responses=[FakeResponse(403, text="Forbidden")])
    with pytest.raises(api.FatalError):
        make_client(session).search({"q": "x"})
    assert len(session.gets) == 1


def test_retries_are_bounded():
    session = FakeSession(get_responses=[FakeResponse(503)] * api.MAX_ATTEMPTS)
    with pytest.raises(api.RetryExhaustedError):
        make_client(session).search({"q": "x"})


def test_low_remaining_rate_limit_backs_off():
    sleeps = []
    session = FakeSession(get_responses=[FakeResponse(
        200, {"posts": []}, headers={"ratelimit-remaining": "10", "ratelimit-reset": "30"})])
    make_client(session, sleeps).search({"q": "x"})
    assert 31 in sleeps


# ---------- fetch / slim ----------

def test_fetch_follows_cursor_and_sends_window():
    session = FakeSession(get_responses=[
        FakeResponse(200, {"posts": [raw_post("a")], "cursor": "c1"}),
        FakeResponse(200, {"posts": [raw_post("b")]}),
    ])
    posts, capped = api.fetch(make_client(session), "quake", T0, T0 + timedelta(hours=1))
    assert [p["uri"] for p in posts] == ["a", "b"] and not capped
    first, second = session.gets[0][1]["params"], session.gets[1][1]["params"]
    assert first["since"] == "2024-01-01T00:00:00Z" and first["until"] == "2024-01-01T01:00:00Z"
    assert "cursor" not in first and second["cursor"] == "c1"


def test_fetch_reports_capped_when_page_limit_reached():
    session = FakeSession(get_responses=[FakeResponse(200, {"posts": [], "cursor": "c"})] * 3)
    _, capped = api.fetch(make_client(session), "quake", T0, T0 + timedelta(hours=1), max_pages=3)
    assert capped


def test_slim_keeps_schema_and_drops_profile_data():
    out = api.slim(raw_post())
    assert set(out) == {"uri", "createdAt", "indexedAt", "text", "langs", "did",
                        "author_created", "reply", "likes", "reposts"}
    assert out["reply"] is False and out["did"] == "did:plc:abc"
    assert "someone" not in json.dumps(out).lower()   # no handle / display name
    assert api.slim(raw_post(record={"text": "re", "reply": {}}))["reply"] is True


# ---------- backfill: window split and resume ----------

class WindowFakeClient:
    """Search results depend on window length: windows longer than 30 min hit the page limit."""

    def __init__(self):
        self.calls = []

    def search(self, params):
        self.calls.append(params)
        start = datetime.fromisoformat(params["since"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(params["until"].replace("Z", "+00:00"))
        if end - start > timedelta(minutes=30):
            return {"posts": [], "cursor": "more"}
        return {"posts": [raw_post(f"{params['q']}-{params['since']}")]}


def test_window_is_split_until_it_fits(tmp_path):
    done = DoneLog(tmp_path / DONE_FILE_NAME)
    process_window(WindowFakeClient(), "quake", T0, T0 + timedelta(hours=1), done, tmp_path,
                   max_pages=2)
    keys = (tmp_path / DONE_FILE_NAME).read_text(encoding="utf-8").splitlines()
    assert keys == [
        "quake|2024-01-01T00:00:00Z|2024-01-01T00:30:00Z",
        "quake|2024-01-01T00:30:00Z|2024-01-01T01:00:00Z",
        "quake|2024-01-01T00:00:00Z|2024-01-01T01:00:00Z",   # parent after both halves
    ]
    lines = (tmp_path / "2024-01-01.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2 and all(json.loads(x)["uri"].startswith("quake-") for x in lines)


def test_finished_windows_are_skipped_on_resume(tmp_path):
    (tmp_path / DONE_FILE_NAME).write_text(
        window_key("quake", T0, T0 + timedelta(hours=1)) + "\n", encoding="utf-8")
    client = WindowFakeClient()
    run_backfill(client, ["quake", "地震"], T0, T0 + timedelta(hours=1), timedelta(hours=1),
                 tmp_path, sleep=lambda s: None)
    assert {c["q"] for c in client.calls} == {"地震"}   # only the unfinished keyword


def test_daily_files_use_window_start_date(tmp_path):
    start = datetime(2024, 3, 5, 23, 45, tzinfo=timezone.utc)
    process_window(WindowFakeClient(), "quake", start, start + timedelta(minutes=15),
                   DoneLog(tmp_path / DONE_FILE_NAME), tmp_path)
    assert (tmp_path / "2024-03-05.jsonl").exists()


def test_backfill_retries_window_after_network_error(tmp_path):
    class FlakyClient(WindowFakeClient):
        def search(self, params):
            if not self.calls:
                self.calls.append(params)
                raise api.RetryExhaustedError("boom")
            return super().search(params)

    sleeps = []
    run_backfill(FlakyClient(), ["quake"], T0, T0 + timedelta(minutes=30), timedelta(minutes=30),
                 tmp_path, sleep=sleeps.append, wait_after_error=1)
    assert sleeps == [1]
    assert (tmp_path / "2024-01-01.jsonl").exists()


# ---------- keyword config ----------

def test_keyword_config_loads():
    cfg = load_keywords()
    assert "earthquake" in cfg.search_keywords and "quake" in cfg.search_keywords
    assert len(cfg.search_keywords) == len(set(cfg.search_keywords))
    assert all(t == t.lower() for t in cfg.exclude_terms)
    # exclusion terms must never exclude a plain earthquake keyword
    assert not any(t in kw.lower() for t in cfg.exclude_terms for kw in cfg.search_keywords)
