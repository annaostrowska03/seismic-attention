import sys
from pathlib import Path

from pyspark.sql import SparkSession

RAW = Path("/opt/app/data/raw")

spark = (
    SparkSession.builder.appName("verify-history")
    .config("spark.sql.session.timeZone", "UTC")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def local_rows(pattern, header=False):
    rows = 0
    for path in RAW.glob(pattern):
        with open(path, encoding="utf-8") as f:
            rows += sum(1 for line in f if line.strip()) - header
    return rows


def value(sql):
    return spark.sql(sql).first()[0]


# (check, result, expected)
checks = [
    ("usgs rows = downloaded events",
     value("SELECT count(*) FROM seismic.usgs_history"), local_rows("usgs_history/*.jsonl")),
    ("gdelt rows = downloaded articles",
     value("SELECT count(*) FROM seismic.gdelt_history"), local_rows("gdelt_history/articles/*.jsonl")),
    ("gdelt file log rows = downloaded statuses",
     value("SELECT count(*) FROM seismic.gdelt_files"), local_rows("gdelt_history/files/*.csv", header=True)),

    ("usgs duplicate ids",
     value("SELECT count(*) - count(DISTINCT id) FROM seismic.usgs_history"), 0),
    ("usgs empty id or time",
     value("SELECT count(*) FROM seismic.usgs_history WHERE id IS NULL OR time IS NULL"), 0),
    ("usgs magnitude empty or below 4.5",
     value("SELECT count(*) FROM seismic.usgs_history WHERE mag IS NULL OR mag < 4.5"), 0),
    ("usgs coordinates out of range",
     value("SELECT count(*) FROM seismic.usgs_history "
           "WHERE NOT (latitude BETWEEN -90 AND 90) OR NOT (longitude BETWEEN -180 AND 180)"), 0),
    ("usgs depth empty or outside -10..800 km",
     value("SELECT count(*) FROM seismic.usgs_history WHERE depth_km IS NULL OR NOT (depth_km BETWEEN -10 AND 800)"), 0),
    ("usgs updated before origin time",
     value("SELECT count(*) FROM seismic.usgs_history WHERE updated < time"), 0),

    ("gdelt duplicate article ids",
     value("SELECT count(*) - count(DISTINCT gkg_id) FROM seismic.gdelt_history"), 0),
    ("gdelt articles without earthquake theme",
     value("SELECT count(*) FROM seismic.gdelt_history WHERE themes NOT LIKE '%EARTHQUAKE%'"), 0),
    ("gdelt locations out of range",
     value("SELECT count(*) FROM seismic.gdelt_history LATERAL VIEW explode(locations) t AS loc "
           "WHERE NOT (loc.latitude BETWEEN -90 AND 90) OR NOT (loc.longitude BETWEEN -180 AND 180)"), 0),
    ("gdelt articles not from an ok file",
     value("SELECT count(*) FROM seismic.gdelt_history a LEFT ANTI JOIN "
           "(SELECT time FROM seismic.gdelt_files WHERE status = 'ok') f ON a.time = f.time"), 0),

    ("gdelt file status other than ok/missing/broken",
     value("SELECT count(*) FROM seismic.gdelt_files WHERE status NOT IN ('ok', 'missing', 'broken')"), 0),
    ("gdelt days without 96 files",
     value("SELECT count(*) FROM (SELECT to_date(time), count(*) AS n FROM seismic.gdelt_files "
           "GROUP BY to_date(time)) WHERE n != 96"), 0),
    ("gdelt articles in missing/broken files",
     value("SELECT count(*) FROM seismic.gdelt_files WHERE status != 'ok' AND articles != 0"), 0),
]

failed = 0
for name, result, expected in checks:
    ok = result == expected
    failed += not ok
    print(f"{'OK  ' if ok else 'FAIL'} {name}: {result} (expected {expected})")

spark.stop()
sys.exit(1 if failed else 0)
