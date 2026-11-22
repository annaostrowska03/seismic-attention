"""Media-coverage labels of the historical earthquakes (rule and gap handling: src/seismic_attention/labels.py).

Run (stack running, history loaded with scripts/load_history.py):
    docker compose exec spark-master /opt/spark/bin/spark-submit /opt/app/scripts/spark_coverage_labels.py

Reads   seismic.usgs_history, seismic.gdelt_history, seismic.gdelt_files
Writes  /curated/coverage_labels (Parquet) + external table seismic.coverage_labels, one row per earthquake.
covered = True / False when the 48 h GDELT window is complete enough, NULL (unknown) otherwise.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/app/src")
from seismic_attention.config import PERIOD_END, PERIOD_START
from seismic_attention.labels import FILES_PER_WINDOW, MIN_FILES, RADIUS_KM, WINDOW_H, article_places, coverage

PATH = "/curated/coverage_labels"
TABLE = "seismic.coverage_labels"
COLUMNS = ["id", "time", "mag", "latitude", "longitude", "depth_km", "place",
           "n_articles", "min_dist_km", "files_ok", "gdelt_complete", "aftershock", "covered"]

spark = (
    SparkSession.builder.appName("coverage-labels")
    .config("spark.sql.session.timeZone", "UTC")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def in_period(c):
    return (F.col(c) >= F.lit(PERIOD_START).cast("timestamp")) & (F.col(c) < F.lit(PERIOD_END).cast("timestamp"))


quakes = (
    spark.table("seismic.usgs_history")
    .where((F.col("type") == "earthquake") & in_period("time"))
    .dropDuplicates(["id"])
)
articles = article_places(spark.table("seismic.gdelt_history").where(in_period("time")))
files = spark.table("seismic.gdelt_files").where(in_period("time"))

labels = coverage(quakes, articles, files).select(*COLUMNS)
labels.coalesce(1).write.mode("overwrite").parquet(PATH)

columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in labels.schema)
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(f"CREATE EXTERNAL TABLE {TABLE} ({columns}) STORED AS PARQUET LOCATION '{PATH}' "
          f"TBLPROPERTIES ('comment' = 'GDELT coverage {WINDOW_H} h / {RADIUS_KM} km, label needs "
          f">= {MIN_FILES} of {FILES_PER_WINDOW} files')")

saved = spark.table(TABLE)
print(f"{TABLE}: {saved.count()} earthquakes {PERIOD_START} - {PERIOD_END}")
saved.groupBy("covered", "aftershock").count().orderBy("covered", "aftershock").show()

spark.stop()
