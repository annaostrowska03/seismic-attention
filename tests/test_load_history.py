import json
import os
import shutil

import pytest

if not (shutil.which("java") or os.environ.get("JAVA_HOME")):
    pytest.skip("Java not found - run Spark tests with: python scripts/run_spark_tests.py",
                allow_module_level=True)

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import load_history


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("test-load-history")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.jars.packages", "")
        .getOrCreate()
    )
    yield session
    session.stop()


def read_json(spark, records):
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in records]))


def as_text(column):
    return F.date_format(column, "yyyy-MM-dd HH:mm:ss.SSS")


def test_usgs_coordinates_time_and_types(spark):
    feature = {
        "type": "Feature", "id": "us7000tymu",
        "properties": {"mag": 6, "magType": "mww", "place": "151 km WSW of Tual, Indonesia",
                       "time": 1794863477168, "updated": 1794864349040, "type": "earthquake",
                       "status": "reviewed", "net": "us", "sig": 670, "tsunami": 0, "felt": None, "alert": "green"},
        "geometry": {"type": "Point", "coordinates": [131.4461, -6.0288, 35]},
    }
    row = (load_history.usgs_table(read_json(spark, [feature]))
           .withColumn("time", as_text("time")).withColumn("updated", as_text("updated")).first())
    # GeoJSON order is [longitude, latitude, depth]
    assert (row.latitude, row.longitude, row.depth_km) == (-6.0288, 131.4461, 35.0)
    assert row.time == "2026-11-16 21:11:17.168"
    assert row.updated == "2026-11-16 21:25:49.040"
    assert row.mag == 6.0 and row.mag_type == "mww"


def test_gdelt_locations_and_time(spark):
    article = {
        "gkg_id": "20250101001500-7", "date": "20250101001500", "source": "example.com",
        "url": "https://example.com/a", "themes": "NATURAL_DISASTER_EARTHQUAKE;",
        "locations": "1#Chile#CI#CI#-30#-71#CI;4#Ancud, Los Lagos, Chile#CI#CI11#-41.87#-73.83#-880001;",
    }
    row = load_history.gdelt_table(read_json(spark, [article])).withColumn("time", as_text("time")).first()
    assert row.time == "2025-01-01 00:15:00.000"
    assert [(p.name, p.country, p.latitude, p.longitude) for p in row.locations] == [
        ("Chile", "CI", -30.0, -71.0),
        ("Ancud, Los Lagos, Chile", "CI", -41.87, -73.83),
    ]


def test_gdelt_without_locations(spark):
    article = {"gkg_id": "x", "date": "20250101001500", "source": "s", "url": "u", "themes": "EARTHQUAKE", "locations": ""}
    assert load_history.gdelt_table(read_json(spark, [article])).first().locations == []


def test_file_log_types(spark):
    raw = spark.createDataFrame([("20250601000000", "missing", "0"), ("20250601001500", "ok", "12")],
                                ["file", "status", "articles"])
    rows = load_history.files_table(raw).withColumn("time", as_text("time")).collect()
    assert [(r.time, r.status, r.articles) for r in rows] == [
        ("2025-06-01 00:00:00.000", "missing", 0),
        ("2025-06-01 00:15:00.000", "ok", 12),
    ]
