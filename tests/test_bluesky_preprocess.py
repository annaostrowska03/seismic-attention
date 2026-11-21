"""Tests of the Spark pre-processing rules for Bluesky posts.

Needs Java. Without it the module is skipped; run it in the Spark container instead:
    python scripts/run_spark_tests.py
Must stay compatible with Python 3.8 (Spark containers).
"""
import json
import os
import shutil

import pytest

if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    pytest.skip("Java not found - run Spark tests with: python scripts/run_spark_tests.py",
                allow_module_level=True)

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from seismic_attention.preprocess import bluesky as bp  # noqa: E402

START, END = "2024-01-01", "2025-01-01"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("test-bsky-preprocess")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # ignore cluster defaults when run inside the Spark container
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.jars.packages", "")
        .getOrCreate()
    )
    yield session
    session.stop()


def post(uri, created="2024-03-01T10:00:00Z", indexed="2024-03-01T10:00:05Z", text="earthquake!",
         did="did:plc:a"):
    return {"uri": uri, "createdAt": created, "indexedAt": indexed, "text": text, "langs": ["en"],
            "did": did, "author_created": "2023-05-01T00:00:00Z", "reply": False, "likes": 0,
            "reposts": 0}


def write_files(base, files):
    """files: {"2024-03-01.jsonl": [post dict or raw line str, ...]}"""
    base.mkdir(parents=True, exist_ok=True)
    for name, rows in files.items():
        lines = [r if isinstance(r, str) else json.dumps(r, ensure_ascii=False) for r in rows]
        # Windows line endings, like the real files
        (base / name).write_bytes(("\r\n".join(lines) + "\r\n").encode("utf-8"))
    return "file://" + str(base)


def flagged(spark, tmp_path, rows, exclude=(), keywords=()):
    path = write_files(tmp_path / "raw", {"2024-03-01.jsonl": rows})
    df = bp.add_flags(bp.deduplicate(bp.read_raw(spark, path)), START, END,
                      exclude_terms=exclude, keywords=keywords)
    return {r["uri"]: r for r in df.collect()}


# ---------- reading ----------

def test_read_raw_ignores_other_files_and_counts_corrupt_lines(spark, tmp_path):
    path = write_files(tmp_path / "raw", {
        "2024-03-01.jsonl": [post("a"), "{not json"],
        "2024-03-02.jsonl": [post("b")],
        "_done.txt": ["earthquake|2024-03-01T00:00:00Z|2024-03-01T06:00:00Z"],
    })
    raw = bp.read_raw(spark, path)
    rows = raw.collect()
    assert len(rows) == 3
    assert sum(r["_corrupt_record"] is not None for r in rows) == 1
    assert {r["file_date"] for r in rows} == {"2024-03-01", "2024-03-02"}
    assert bp.summarize(raw, bp.add_flags(bp.deduplicate(raw), START, END))["corrupt_rows"] == 1


def test_read_raw_finds_files_in_hdfs_style_partition_dirs(spark, tmp_path):
    write_files(tmp_path / "raw" / "date=2024-03-01", {"2024-03-01.jsonl": [post("a")]})
    raw = bp.read_raw(spark, "file://" + str(tmp_path / "raw"))
    assert [r["file_date"] for r in raw.collect()] == ["2024-03-01"]


# ---------- deduplication ----------

def test_duplicates_are_removed_by_uri_and_earliest_file_wins(spark, tmp_path):
    path = write_files(tmp_path / "raw", {
        "2024-03-02.jsonl": [post("a", text="later copy")],
        "2024-03-01.jsonl": [post("a", text="first copy"), post("a", text="first copy"), post("b")],
    })
    rows = {r["uri"]: r for r in bp.deduplicate(bp.read_raw(spark, path)).collect()}
    assert set(rows) == {"a", "b"}
    assert rows["a"]["n_copies"] == 3 and rows["a"]["file_date"] == "2024-03-01"
    assert rows["b"]["n_copies"] == 1


# ---------- event time rule ----------

def test_t_event_rule(spark, tmp_path):
    r = flagged(spark, tmp_path, [
        post("ok", "2024-03-01T10:00:00Z", "2024-03-01T10:00:05Z"),
        post("old_client_date", "2010-07-30T21:37:21Z", "2024-03-01T09:00:00Z"),
        post("future_5min", "2024-03-01T10:05:00Z", "2024-03-01T10:00:00Z"),       # boundary: ok
        post("future_5min_1s", "2024-03-01T10:05:01Z", "2024-03-01T10:00:00Z"),
        post("garbage_created", "not a date", "2024-03-01T09:00:00Z"),
        post("created_in_2023", "2023-12-31T23:59:50Z", "2024-01-01T00:00:10Z"),
    ])
    assert r["ok"]["time_source"] == "createdAt" and not r["ok"]["bad_created_at"]
    assert str(r["ok"]["t_event"]) == "2024-03-01 10:00:00"
    assert r["old_client_date"]["time_source"] == "indexedAt" and r["old_client_date"]["bad_created_at"]
    assert str(r["old_client_date"]["t_event"]) == "2024-03-01 09:00:00"
    assert r["future_5min"]["time_source"] == "createdAt"
    assert r["future_5min_1s"]["time_source"] == "indexedAt"
    assert r["garbage_created"]["time_source"] == "indexedAt"
    assert r["garbage_created"]["created_ts"] is None
    # createdAt outside the period -> indexedAt, even if it is only seconds earlier
    assert r["created_in_2023"]["time_source"] == "indexedAt" and r["created_in_2023"]["in_period"]


def test_in_period_uses_t_event(spark, tmp_path):
    r = flagged(spark, tmp_path, [
        post("first_second", "2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z"),
        post("indexed_2025", "2010-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
        post("last_second", "2024-12-31T23:59:59Z", "2025-01-01T00:00:02Z"),
    ])
    assert r["first_second"]["in_period"] and r["last_second"]["in_period"]
    assert not r["indexed_2025"]["in_period"]
    assert r["indexed_2025"]["event_month"] == bp.OUT_OF_PERIOD
    assert r["first_second"]["event_month"] == "2024-01"


# ---------- reindexed and clean ----------

def test_reindexed_threshold_is_strictly_more_than_one_day(spark, tmp_path):
    r = flagged(spark, tmp_path, [
        post("exactly_1d", "2024-03-01T10:00:00Z", "2024-03-02T10:00:00Z"),
        post("1d_1s", "2024-03-01T10:00:00Z", "2024-03-02T10:00:01Z"),
    ])
    assert not r["exactly_1d"]["reindexed"]
    assert r["1d_1s"]["reindexed"] and r["1d_1s"]["time_source"] == "createdAt"


def test_clean_lag_window_is_minus5_to_plus10_minutes(spark, tmp_path):
    r = flagged(spark, tmp_path, [
        post("lag_-5", "2024-03-01T10:05:00Z", "2024-03-01T10:00:00Z"),
        post("lag_0", "2024-03-01T10:00:00Z", "2024-03-01T10:00:00Z"),
        post("lag_10", "2024-03-01T10:00:00Z", "2024-03-01T10:10:00Z"),
        post("lag_10_1s", "2024-03-01T10:00:00Z", "2024-03-01T10:10:01Z"),
        post("lag_-5_1s", "2024-03-01T10:05:01Z", "2024-03-01T10:00:00Z"),
        post("bad_created", "2010-01-01T00:00:00Z", "2024-03-01T10:00:00Z"),
    ])
    assert [r[k]["is_clean"] for k in ("lag_-5", "lag_0", "lag_10")] == [True, True, True]
    assert [r[k]["is_clean"] for k in ("lag_10_1s", "lag_-5_1s", "bad_created")] == [False] * 3
    assert r["lag_10"]["lag_min"] == pytest.approx(10.0)


# ---------- keyword columns ----------

def test_unrelated_candidate_is_a_flag_only(spark, tmp_path):
    r = flagged(spark, tmp_path, [
        post("game", text="Playing QUAKE Champions tonight"),
        post("quake", text="M5.1 quake near Tokyo"),
        post("no_text", text=None),
    ], exclude=["quake champions"])
    assert r["game"]["unrelated_candidate"] is True
    assert r["quake"]["unrelated_candidate"] is False
    assert r["no_text"]["unrelated_candidate"] is False
    assert len(r) == 3                                   # nothing removed


def test_matched_keywords(spark, tmp_path):
    r = flagged(spark, tmp_path, [
        post("ja", text="地震と津波に注意"),
        post("en", text="Big EARTHQUAKE"),
        post("none", text="link only"),
    ], keywords=["earthquake", "quake", "地震", "津波"])
    assert r["ja"]["matched_keywords"] == ["地震", "津波"]
    assert r["en"]["matched_keywords"] == ["earthquake", "quake"]
    assert r["none"]["matched_keywords"] == []


# ---------- minute counts and summary ----------

def test_minute_counts_are_dense_and_count_authors(spark, tmp_path):
    path = write_files(tmp_path / "raw", {"2024-03-01.jsonl": [
        post("a", "2024-03-01T10:00:01Z", "2024-03-01T10:00:02Z", did="did:plc:x"),
        post("b", "2024-03-01T10:00:30Z", "2024-03-01T10:00:31Z", did="did:plc:x"),
        post("c", "2024-03-01T10:00:59Z", "2024-03-01T10:20:00Z", did="did:plc:y"),  # not clean
        post("d", "2024-03-01T10:02:00Z", "2024-03-01T10:02:01Z", did="did:plc:z"),
    ]})
    posts = bp.add_flags(bp.deduplicate(bp.read_raw(spark, path)),
                         "2024-03-01T10:00:00Z", "2024-03-01T11:00:00Z")
    minutes = bp.minute_counts(posts, "2024-03-01T10:00:00Z", "2024-03-01T11:00:00Z").collect()
    assert len(minutes) == 60
    first = minutes[0]
    assert (first["n_posts"], first["n_authors"], first["n_posts_clean"], first["n_authors_clean"]) \
        == (3, 2, 2, 1)
    assert minutes[1]["n_posts"] == 0 and minutes[2]["n_posts"] == 1
    assert sum(m["n_posts"] for m in minutes) == 4


def test_summary_control_numbers(spark, tmp_path):
    path = write_files(tmp_path / "raw", {"2024-03-01.jsonl": [
        post("a"), post("a"), post("b", "2010-01-01T00:00:00Z", "2024-03-01T10:00:00Z"),
        post("c", "2010-01-01T00:00:00Z", "2025-02-01T00:00:00Z"),
    ]})
    raw = bp.read_raw(spark, path)
    s = bp.summarize(raw, bp.add_flags(bp.deduplicate(raw), START, END))
    assert (s["raw_rows"], s["unique_posts"], s["in_period"], s["out_of_period"]) == (4, 3, 2, 1)
    assert s["in_period_time_source_createdAt"] == 1 and s["in_period_time_source_indexedAt"] == 1
    assert s["clean"] == 1 and s["raw_files"] == 1
