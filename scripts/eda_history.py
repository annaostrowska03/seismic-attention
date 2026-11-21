import csv
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/app/src")
from seismic_attention.features import SPARK_SCHEMA, usgs_features
from seismic_attention.labels import MIN_FILES, WINDOW_H, article_places
from seismic_attention.labels import coverage as label_coverage

RADII_KM = [100, 150, 200, 250]
RADIUS_KM = max(RADII_KM)
START, END = "2024-01-01", "2026-11-21"
OUT_DIR = "/opt/app/data/processed/eda_history"

spark = (
    SparkSession.builder.appName("eda-history")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.executorEnv.PYTHONPATH", "/opt/app/src")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def bin_floor(col, edges):
    """Lower edge of the bin the value falls into; values below edges[0] go to the first bin."""
    expr = F.lit(edges[0])
    for edge in edges[1:]:
        expr = F.when(col >= edge, edge).otherwise(expr)
    return F.when(col.isNotNull(), expr)


report_lines = []


def save_csv(name, df):
    rows = df.collect()
    with open(f"{OUT_DIR}/{name}.csv", "w", newline="", encoding="utf-8") as f:
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
articles = article_places(spark.table("seismic.gdelt_history").where(in_range("time")))

files = spark.table("seismic.gdelt_files").where(in_range("time"))

# 1. earthquakes per month by magnitude
report("quakes_per_month", quakes.groupBy("month").pivot("mag_bin").count().na.fill(0).orderBy("month"),
       "Earthquakes per month by magnitude (column = floor(mag), 7 = M7+)")

# records per day for filtering
usgs_daily = quakes.groupBy(F.to_date("time").alias("day")).agg(F.count("*").alias("n"))
gdelt_daily = (
    files.where(F.col("status") == "ok")
    .groupBy(F.to_date("time").alias("day"))
    .agg(F.count("*").alias("files"), F.sum("articles").alias("n"))
    .where(F.col("files") == 96)
)


def per_day(name, daily):
    return daily.agg(
        F.lit(name).alias("records"),
        F.count("*").alias("days"),
        F.round(F.avg("n"), 1).alias("avg_per_day"),
        F.min("n").alias("min"),
        F.max("n").alias("max"),
    )


report("records_per_day",
       per_day("usgs earthquakes M4.5+", usgs_daily).unionByName(per_day("gdelt earthquake articles", gdelt_daily)),
       "Records per day")

# USGS data quality
usgs_raw = spark.table("seismic.usgs_history").where(in_range("time"))
report("usgs_quality", usgs_raw.agg(
    F.count("*").alias("rows"),
    (F.count("*") - F.countDistinct("id")).alias("duplicate_ids"),
    F.sum((F.col("type") != "earthquake").cast("int")).alias("not_earthquake"),
    F.sum((F.col("status") == "reviewed").cast("int")).alias("reviewed"),
    F.sum((F.col("status") == "automatic").cast("int")).alias("automatic"),
    F.sum(F.col("mag").isNull().cast("int")).alias("no_mag"),
    F.sum(F.col("depth_km").isNull().cast("int")).alias("no_depth"),
    F.sum((F.col("place").isNull() | (F.col("place") == "")).cast("int")).alias("no_place"),
), "USGS catalogue quality")
report("usgs_mag_type", usgs_raw.groupBy("mag_type").count().orderBy(F.desc("count")),
       "USGS magnitude types")

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

# 3. quake-article matching, complete GDELT windows and aftershocks (src/seismic_attention/labels.py):
# matches up to RADIUS_KM, so min_dist_km <= r gives the label for each smaller radius r
coverage = label_coverage(quakes, articles, files, WINDOW_H, RADIUS_KM, MIN_FILES)
for r in RADII_KM:
    coverage = coverage.withColumn(f"cov_{r}", F.coalesce(F.col("min_dist_km") <= r, F.lit(False)))
coverage = (
    coverage
    .withColumn("depth_bin_km", bin_floor(F.col("depth_km"), [0, 10, 35, 70, 150, 300]))
    .withColumn("place_dist_bin_km", bin_floor(F.col("place_distance_km"), [0, 10, 25, 50, 100, 200]))
    .cache()
)
coverage.select("id", "time", "mag", "depth_km", "latitude", "longitude", "place_region",
                "min_dist_km", "n_articles", "gdelt_complete", "aftershock", *[f"cov_{r}" for r in RADII_KM]) \
    .coalesce(1).write.mode("overwrite").parquet(f"file://{OUT_DIR}/quake_coverage")

report("quakes_used", coverage.agg(
    F.count("*").alias("quakes"),
    F.sum(F.col("gdelt_complete").cast("int")).alias("complete_gdelt_window"),
    F.sum(F.col("aftershock").cast("int")).alias("aftershocks"),
    F.sum((F.col("gdelt_complete") & ~F.col("aftershock")).cast("int")).alias("used"),
), "Earthquakes used below: complete GDELT window and not an aftershock")

labelled = coverage.where(F.col("gdelt_complete") & ~F.col("aftershock")).cache()

# class balance of the label (covered / not covered) for each radius
balance = None
for r in RADII_KM:
    part = labelled.agg(
        F.lit(r).alias("radius_km"),
        F.sum(F.col(f"cov_{r}").cast("int")).alias("covered"),
        F.sum((~F.col(f"cov_{r}")).cast("int")).alias("not_covered"),
        F.round(100 * F.avg(F.col(f"cov_{r}").cast("int")), 1).alias("pct_covered"),
    )
    balance = part if balance is None else balance.unionByName(part)
report("class_balance", balance, "Class balance of the coverage label")


def coverage_by(dim, order=None):
    aggs = [F.count("*").alias("quakes")] + [
        F.round(100 * F.avg(F.col(f"cov_{r}").cast("int")), 1).alias(f"pct_{r}km") for r in RADII_KM
    ]
    return labelled.groupBy(dim).agg(*aggs).orderBy(order if order is not None else dim)


title = f"% of earthquakes with a GDELT article within {WINDOW_H}h mentioning a place within R km"
report("coverage_total", coverage_by(F.lit("all").alias("all")), f"{title} - overall")
report("coverage_by_mag", coverage_by("mag_bin"), f"{title} - by magnitude (floor, 7 = M7+)")
report("coverage_by_month", coverage_by("month"), f"{title} - by month")
report("coverage_by_region", coverage_by("place_region", F.desc("quakes")), f"{title} - by region", limit=25)
report("coverage_by_place_distance", coverage_by("place_dist_bin_km"),
       f"{title} - by distance to the named place (bin lower edge, km)")
report("coverage_by_depth", coverage_by("depth_bin_km"), f"{title} - by depth (bin lower edge, km)")
report("coverage_by_hour_local", coverage_by("hour_local"), f"{title} - by local solar hour")

with open(f"{OUT_DIR}/report.md", "w", encoding="utf-8") as f:
    f.write(f"# EDA history {START} - {END}\n\n" + "\n".join(report_lines))
print(f"saved to {OUT_DIR}")

spark.stop()
