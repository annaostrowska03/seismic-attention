"""Tests of the coverage label rules (src/seismic_attention/labels.py).

Needs Java. Without it the module is skipped; run it in the Spark container instead:
    python scripts/run_spark_tests.py
Must stay compatible with Python 3.8 (Spark containers).
"""
import math
import os
import shutil
from datetime import datetime, timedelta

import pytest

if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    pytest.skip("Java not found - run Spark tests with: python scripts/run_spark_tests.py",
                allow_module_level=True)

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from seismic_attention import labels  # noqa: E402

T0 = datetime(2025, 3, 24, 0, 0, 0)
KM_PER_DEG = labels.EARTH_RADIUS_KM * math.pi / 180   # length of 1 degree of latitude, about 111.2 km


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("test-labels")
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


def ts(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def quakes_df(spark, rows):
    """rows: (id, time, mag, latitude, longitude)"""
    df = spark.createDataFrame([(i, ts(t), float(m), float(la), float(lo)) for i, t, m, la, lo in rows],
                               "id string, time string, mag double, latitude double, longitude double")
    return df.withColumn("time", F.to_timestamp("time"))


def articles_df(spark, rows):
    """rows: (gkg_id, time, lat, lon) - one row per (article, place)"""
    df = spark.createDataFrame([(g, ts(t), float(la), float(lo)) for g, t, la, lo in rows],
                               "gkg_id string, time string, lat double, lon double")
    return df.withColumn("time", F.to_timestamp("time"))


def files_df(spark, start, n, status="ok"):
    """n GDELT files every 15 minutes from start"""
    rows = [(ts(start + timedelta(minutes=15 * k)), status) for k in range(n)]
    return spark.createDataFrame(rows, "time string, status string").withColumn("time", F.to_timestamp("time"))


def by_id(df):
    return {r["id"]: r for r in df.collect()}


# ---------- distance ----------

def test_distance_same_point_is_zero(spark):
    row = spark.range(1).select(labels.distance_km(F.lit(10.0), F.lit(20.0), F.lit(10.0), F.lit(20.0)).alias("d"))
    assert row.first()["d"] == pytest.approx(0.0, abs=1e-9)


def test_distance_one_degree_of_latitude(spark):
    row = spark.range(1).select(labels.distance_km(F.lit(0.0), F.lit(0.0), F.lit(1.0), F.lit(0.0)).alias("d"))
    assert row.first()["d"] == pytest.approx(111.19, abs=0.01)


def test_distance_shrinks_with_latitude(spark):
    # 1 degree of longitude at 60N is half of that at the equator
    row = spark.range(1).select(labels.distance_km(F.lit(60.0), F.lit(0.0), F.lit(60.0), F.lit(1.0)).alias("d"))
    assert row.first()["d"] == pytest.approx(111.19 / 2, abs=0.1)


# ---------- article places ----------

def test_article_places_drops_places_without_coordinates_and_duplicates(spark):
    gdelt = spark.createDataFrame(
        [("a1", ts(T0), [("Tokyo", "JA", 35.7, 139.7), ("Japan", "JA", None, None), ("Tokyo", "JA", 35.7, 139.7)])],
        "gkg_id string, time string, "
        "locations array<struct<name:string,country:string,latitude:double,longitude:double>>",
    ).withColumn("time", F.to_timestamp("time"))
    rows = labels.article_places(gdelt).collect()
    assert [(r["gkg_id"], r["lat"], r["lon"]) for r in rows] == [("a1", 35.7, 139.7)]


# ---------- matching: time window and radius ----------

def test_match_time_window_includes_both_ends(spark):
    quakes = quakes_df(spark, [("q", T0 + timedelta(hours=1), 5.0, 0, 0)])
    articles = articles_df(spark, [
        ("at_start", T0 + timedelta(hours=1), 0, 0),
        ("at_48h", T0 + timedelta(hours=49), 0, 0),
        ("after_48h", T0 + timedelta(hours=49, seconds=1), 0, 0),
        ("before", T0 + timedelta(minutes=59), 0, 0),
    ])
    m = by_id(labels.match_articles(quakes, articles))
    assert m["q"]["n_articles"] == 2   # at_start and at_48h


def test_match_radius_250_km(spark):
    quakes = quakes_df(spark, [("near", T0, 5.0, 0, 0), ("far", T0, 5.0, 0, 50)])
    articles = articles_df(spark, [
        ("a249", T0 + timedelta(hours=1), 249 / KM_PER_DEG, 0),
        ("a251", T0 + timedelta(hours=1), 251 / KM_PER_DEG, 50),
    ])
    m = by_id(labels.match_articles(quakes, articles))
    assert m["near"]["n_articles"] == 1
    assert m["near"]["min_dist_km"] == pytest.approx(249, abs=0.01)
    assert "far" not in m   # 251 km away


def test_match_counts_articles_not_places_and_takes_nearest(spark):
    quakes = quakes_df(spark, [("q", T0, 5.0, 0, 0)])
    articles = articles_df(spark, [
        ("a1", T0 + timedelta(hours=1), 100 / KM_PER_DEG, 0),
        ("a1", T0 + timedelta(hours=1), 10 / KM_PER_DEG, 0),   # same article, second place
        ("a2", T0 + timedelta(hours=2), 50 / KM_PER_DEG, 0),
    ])
    m = by_id(labels.match_articles(quakes, articles))
    assert m["q"]["n_articles"] == 2
    assert m["q"]["min_dist_km"] == pytest.approx(10, abs=0.01)


def test_match_across_latitude_band_border(spark):
    # the join key splits latitudes into bands of RADIUS_KM / 111 degrees; a place 2 km away
    # but in the next band must still match
    border = labels.RADIUS_KM / labels.KM_PER_DEG_LAT
    quakes = quakes_df(spark, [("q", T0, 5.0, border - 0.01, 20)])
    articles = articles_df(spark, [("a", T0 + timedelta(hours=1), border + 0.01, 20)])
    m = by_id(labels.match_articles(quakes, articles))
    assert m["q"]["n_articles"] == 1


def test_match_window_over_midnight(spark):
    # quake late in the day, article two calendar days later
    quakes = quakes_df(spark, [("q", T0 + timedelta(hours=23, minutes=50), 5.0, 0, 0)])
    articles = articles_df(spark, [("a", T0 + timedelta(hours=71), 0, 0)])
    assert by_id(labels.match_articles(quakes, articles))["q"]["n_articles"] == 1


# ---------- GDELT files in the window ----------

def test_window_files_counts_only_ok_files_in_window(spark):
    quakes = quakes_df(spark, [("q", T0, 5.0, 0, 0)])
    files = (files_df(spark, T0 - timedelta(hours=1), 4)                 # before the quake
             .unionByName(files_df(spark, T0, 10))                       # in the window
             .unionByName(files_df(spark, T0 + timedelta(hours=3), 2, "missing")))
    assert by_id(labels.window_files(quakes, files))["q"]["files_ok"] == 10


def test_full_window_has_193_files_with_both_ends(spark):
    # a quake exactly on a file time sees the files at +0 h and +48 h
    quakes = quakes_df(spark, [("q", T0, 5.0, 0, 0)])
    files = files_df(spark, T0 - timedelta(hours=24), 4 * 96)
    assert by_id(labels.window_files(quakes, files))["q"]["files_ok"] == labels.FILES_PER_WINDOW + 1


# ---------- the label ----------

def label_for(spark, n_files, with_article):
    quakes = quakes_df(spark, [("q", T0, 5.0, 0, 0)])
    articles = articles_df(spark, [("a", T0 + timedelta(hours=1), 0, 0)] if with_article else
                           [("a", T0 + timedelta(hours=1), 50, 50)])   # far away: no match
    return by_id(labels.coverage(quakes, articles, files_df(spark, T0, n_files)))["q"]


def test_label_threshold_173_files(spark):
    assert label_for(spark, 172, False)["covered"] is None
    assert label_for(spark, 173, False)["covered"] is False
    assert label_for(spark, 173, True)["covered"] is True


def test_incomplete_window_gives_null_also_for_covered_quake(spark):
    # decision 3a: windows with gaps get no label for either class, so gaps do not change the class balance
    row = label_for(spark, 100, True)
    assert row["covered"] is None
    assert row["n_articles"] == 1
    assert row["gdelt_complete"] is False


def test_quake_without_files_or_articles(spark):
    quakes = quakes_df(spark, [("q", T0, 5.0, 0, 0)])
    row = by_id(labels.coverage(quakes, articles_df(spark, []), files_df(spark, T0, 0)))["q"]
    assert (row["files_ok"], row["n_articles"], row["min_dist_km"], row["covered"]) == (0, 0, None, None)


def test_coverage_keeps_extra_quake_columns(spark):
    quakes = quakes_df(spark, [("q", T0, 5.0, 0, 0)]).withColumn("place", F.lit("somewhere"))
    row = by_id(labels.coverage(quakes, articles_df(spark, []), files_df(spark, T0, 0)))["q"]
    assert row["place"] == "somewhere"


# ---------- aftershocks ----------

def test_aftershocks(spark):
    # each case in its own region (40 degrees of longitude apart), so the cases do not affect each other
    near = 100 / KM_PER_DEG
    quakes = quakes_df(spark, [
        ("a_main", T0, 6.0, 0, 0),
        ("a_after", T0 + timedelta(hours=47), 5.0, near, 0),            # weaker, 47 h later, 100 km: aftershock
        ("b_main", T0, 6.0, 0, 40),
        ("b_equal", T0 + timedelta(hours=1), 6.0, near, 40),            # same magnitude counts as well
        ("c_main", T0, 6.0, 0, 80),
        ("c_late", T0 + timedelta(hours=49), 5.0, near, 80),            # more than 48 h later: independent
        ("d_main", T0, 6.0, 0, 120),
        ("d_far", T0 + timedelta(hours=1), 5.0, 300 / KM_PER_DEG, 120),  # 300 km away: independent
        ("e_small", T0, 5.0, 0, 160),
        ("e_bigger", T0 + timedelta(hours=1), 6.0, near, 160),          # stronger than the earlier one: independent
    ])
    ids = {r["id"] for r in labels.aftershocks(quakes).collect()}
    assert ids == {"a_after", "b_equal"}   # a quake is never an aftershock of a later one (a_main, e_small)


def test_aftershock_flag_in_coverage(spark):
    quakes = quakes_df(spark, [("main", T0, 6.0, 0, 0), ("after", T0 + timedelta(hours=1), 5.0, 0, 0)])
    rows = by_id(labels.coverage(quakes, articles_df(spark, []), files_df(spark, T0, 0)))
    assert (rows["main"]["aftershock"], rows["after"]["aftershock"]) == (False, True)
