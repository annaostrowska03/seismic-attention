import csv
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/app/src")
from seismic_attention.features import SPARK_SCHEMA, usgs_features

RADII_KM = [100, 150, 200, 250]
RADIUS_KM = max(RADII_KM)
WINDOW_H = 48
START, END = "2024-01-01", "2026-11-21"
EARTH_RADIUS_KM = 6371.0
KM_PER_DEG_LAT = 111.0
OUT_DIR = "/opt/app/data/processed/eda_history"

spark = (
    SparkSession.builder.appName("eda-history")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.executorEnv.PYTHONPATH", "/opt/app/src")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def distance_km(lat1, lon1, lat2, lon2):
    # haversine distance between (lat1, lon1) and (lat2, lon2); args are Columns (degrees)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def bin_floor(col, edges):
    """Lower edge of the bin the value falls into; values below edges[0] go to the first bin."""
    expr = F.lit(edges[0])
    for edge in edges[1:]:
        expr = F.when(col >= edge, edge).otherwise(expr)
    return F.when(col.isNotNull(), expr)


report_lines = []


def save_csv(name, df):
    rows = df.collect()
    with open(f"{OUT_DIR}/{name}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows(rows)
    return rows


def report(name, df, title, limit=None):
    """Prints df as a markdown table, adds it to report.md and saves all rows to <name>.csv."""
    rows = save_csv(name, df)
    cols = df.columns
    shown = rows[:limit] if limit else rows
    cells = [["-" if v is None else str(v) for v in row] for row in shown]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(cols)]

    def line(values):
        return "| " + " | ".join(v.rjust(w) for v, w in zip(values, widths)) + " |"

    lines = [f"## {title}", "", line(cols), "|" + "|".join("-" * (w + 2) for w in widths) + "|"]
    lines += [line(r) for r in cells]
    if limit and len(rows) > limit:
        lines.append(f"... {len(rows) - limit} more rows in {name}.csv")
    lines.append("")
    print("\n".join(lines))
    report_lines.extend(lines)


os.makedirs(OUT_DIR, exist_ok=True)


def in_range(c):
    return (F.col(c) >= F.lit(START).cast("timestamp")) & (F.col(c) < F.lit(END).cast("timestamp"))


features = F.udf(usgs_features, SPARK_SCHEMA)
quakes = (
    spark.table("seismic.usgs_history")
    .where((F.col("type") == "earthquake") & in_range("time"))
    .dropDuplicates(["id"])
    .withColumn("f", features("mag", "depth_km", "latitude", "longitude", F.unix_millis("time"), "place"))
    .select("id", "time", "f.*")
    .withColumn("month", F.date_format("time", "yyyy-MM"))
    .withColumn("mag_bin", F.least(F.floor("mag"), F.lit(7)).cast("int"))
    .cache()
)

# one row per (article, mentioned location)
articles = (
    spark.table("seismic.gdelt_history")
    .where(in_range("time"))
    .select("gkg_id", "time", F.explode("locations").alias("loc"))
    .select("gkg_id", "time",
            F.col("loc.latitude").alias("lat"), F.col("loc.longitude").alias("lon"))
    .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    .dropDuplicates()
)

files = spark.table("seismic.gdelt_files").where(in_range("time"))

# 1. earthquakes per month by magnitude
report("quakes_per_month", quakes.groupBy("month").pivot("mag_bin").count().na.fill(0).orderBy("month"),
       "Earthquakes per month by magnitude (column = floor(mag), 7 = M7+)")

# 2. missing GDELT files: expected one per 15 minutes; missing = never downloaded or status != ok
expected = spark.sql(f"""
    SELECT explode(sequence(timestamp'{START}', timestamp'{END}' - INTERVAL 15 MINUTES, INTERVAL 15 MINUTES)) AS time
""")
missing = (
    expected.join(files.where(F.col("status") == "ok"), "time", "left_anti")
    .join(files.select("time", "status"), "time", "left")
    .withColumn("status", F.coalesce("status", F.lit("not downloaded")))
)
save_csv("gdelt_missing_files", missing.orderBy("time"))
report("gdelt_missing_per_month",
       missing.groupBy(F.date_format("time", "yyyy-MM").alias("month")).pivot("status").count()
       .na.fill(0).orderBy("month"),
       f"Missing GDELT files per month (of ~{96 * 30} expected; full list in gdelt_missing_files.csv)")

# 3. quake-article matching
# Equi-join key (day, latitude band) so Spark can hash-join instead of comparing every pair:
# a 48h window touches at most 3 calendar days, and a band wider than RADIUS_KM means
# a match is always in the same or a neighbouring band.
band_deg = RADIUS_KM / KM_PER_DEG_LAT
q_keys = (
    quakes.select("id", F.col("time").alias("q_time"), F.col("latitude").alias("q_lat"),
                  F.col("longitude").alias("q_lon"))
    .withColumn("day", F.explode(F.sequence(F.to_date("q_time"), F.date_add(F.to_date("q_time"), WINDOW_H // 24))))
    .withColumn("band", F.explode(F.array(*[F.floor(F.col("q_lat") / band_deg) + d for d in (-1, 0, 1)])))
)
a_keys = (
    articles.withColumn("day", F.to_date("time"))
    .withColumn("band", F.floor(F.col("lat") / band_deg))
)
matches = (
    q_keys.join(a_keys, ["day", "band"])
    .where((F.col("time") >= F.col("q_time"))
           & (F.col("time") <= F.col("q_time") + F.expr(f"INTERVAL {WINDOW_H} HOURS")))
    .withColumn("dist_km", distance_km(F.col("q_lat"), F.col("q_lon"), F.col("lat"), F.col("lon")))
    .where(F.col("dist_km") <= RADIUS_KM)
    .groupBy("id")
    .agg(F.min("dist_km").alias("min_dist_km"), F.countDistinct("gkg_id").alias("n_articles"))
)

coverage = quakes.join(matches, "id", "left").na.fill(0, ["n_articles"])
for r in RADII_KM:
    coverage = coverage.withColumn(f"cov_{r}", F.coalesce(F.col("min_dist_km") <= r, F.lit(False)))
coverage = (
    coverage
    .withColumn("depth_bin_km", bin_floor(F.col("depth_km"), [0, 10, 35, 70, 150, 300]))
    .withColumn("place_dist_bin_km", bin_floor(F.col("place_distance_km"), [0, 10, 25, 50, 100, 200]))
    .cache()
)
coverage.select("id", "time", "mag", "depth_km", "latitude", "longitude", "place_region",
                "min_dist_km", "n_articles", *[f"cov_{r}" for r in RADII_KM]) \
    .coalesce(1).write.mode("overwrite").parquet(f"file://{OUT_DIR}/quake_coverage")


def coverage_by(dim, order=None):
    aggs = [F.count("*").alias("quakes")] + [
        F.round(100 * F.avg(F.col(f"cov_{r}").cast("int")), 1).alias(f"pct_{r}km") for r in RADII_KM
    ]
    return coverage.groupBy(dim).agg(*aggs).orderBy(order if order is not None else dim)


title = f"% of earthquakes with a GDELT article within {WINDOW_H}h mentioning a place within R km"
report("coverage_total", coverage_by(F.lit("all").alias("all")), f"{title} - overall")
report("coverage_by_mag", coverage_by("mag_bin"), f"{title} - by magnitude (floor, 7 = M7+)")
report("coverage_by_month", coverage_by("month"), f"{title} - by month")
report("coverage_by_region", coverage_by("place_region", F.desc("quakes")), f"{title} - by region", limit=25)
report("coverage_by_place_distance", coverage_by("place_dist_bin_km"),
       f"{title} - by distance to the named place (bin lower edge, km)")
report("coverage_by_depth", coverage_by("depth_bin_km"), f"{title} - by depth (bin lower edge, km)")
report("coverage_by_hour_local", coverage_by("hour_local"), f"{title} - by local solar hour")

with open(f"{OUT_DIR}/report.md", "w") as f:
    f.write(f"# EDA history {START} - {END}\n\n" + "\n".join(report_lines))
print(f"saved to {OUT_DIR}")

spark.stop()
