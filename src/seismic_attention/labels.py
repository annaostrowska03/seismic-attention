"""Media-coverage label of an earthquake, computed in Spark.

An earthquake is covered if GDELT has at least one earthquake article within WINDOW_H hours after it
that mentions a place within RADIUS_KM km of the epicentre (rule from MS1).
A missing GDELT file can hide an article, so the label is only given when at least MIN_FILES of the
GDELT files in the window were downloaded; otherwise it is NULL (unknown) - for covered and not covered alike,
so that windows with gaps do not change the share of covered earthquakes.
Aftershocks (a stronger or equal quake within RADIUS_KM in the WINDOW_H before) are flagged, not removed:
articles about the main shock would make them look covered.

Used by scripts/spark_coverage_labels.py (label table) and scripts/eda_history.py (exploration).
Input DataFrames:
    quakes:   id, time (timestamp UTC), mag, latitude, longitude (+ any other columns, kept)
    articles: gkg_id, time, lat, lon - one row per (article, mentioned place), see article_places()
    files:    time, status - one row per GDELT file (log), status "ok" = downloaded and read
"""
from pyspark.sql import functions as F

WINDOW_H = 48
RADIUS_KM = 250
FILES_PER_WINDOW = WINDOW_H * 4  # GDELT publishes one file every 15 minutes
MIN_FILES = 173  # 90% of FILES_PER_WINDOW
EARTH_RADIUS_KM = 6371.0
KM_PER_DEG_LAT = 111.0


def distance_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km between (lat1, lon1) and (lat2, lon2); arguments are Columns in degrees."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def article_places(gdelt):
    """gdelt_history (locations = array of structs) -> one row per (article, place with coordinates)."""
    return (
        gdelt.select("gkg_id", "time", F.explode("locations").alias("loc"))
        .select("gkg_id", "time", F.col("loc.latitude").alias("lat"), F.col("loc.longitude").alias("lon"))
        .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .dropDuplicates()
    )


def _days_after(time_col, window_h):
    # calendar days touched by [time, time + window_h]
    return F.explode(F.sequence(F.to_date(time_col), F.date_add(F.to_date(time_col), window_h // 24)))


def match_articles(quakes, articles, window_h=WINDOW_H, radius_km=RADIUS_KM):
    """id, min_dist_km, n_articles for quakes with an article place within radius_km in [time, time + window_h].

    Equi-join key (day, latitude band) so Spark can hash-join instead of comparing every pair:
    a 48 h window touches at most 3 calendar days, and a band wider than radius_km means
    a match is always in the same or a neighbouring band.
    """
    band_deg = radius_km / KM_PER_DEG_LAT
    q_keys = (
        quakes.select("id", F.col("time").alias("q_time"), F.col("latitude").alias("q_lat"),
                      F.col("longitude").alias("q_lon"))
        .withColumn("day", _days_after("q_time", window_h))
        .withColumn("band", F.explode(F.array(*[F.floor(F.col("q_lat") / band_deg) + d for d in (-1, 0, 1)])))
    )
    a_keys = (
        articles.withColumn("day", F.to_date("time"))
        .withColumn("band", F.floor(F.col("lat") / band_deg))
    )
    return (
        q_keys.join(a_keys, ["day", "band"])
        .where((F.col("time") >= F.col("q_time"))
               & (F.col("time") <= F.col("q_time") + F.expr(f"INTERVAL {window_h} HOURS")))
        .withColumn("dist_km", distance_km(F.col("q_lat"), F.col("q_lon"), F.col("lat"), F.col("lon")))
        .where(F.col("dist_km") <= radius_km)
        .groupBy("id")
        .agg(F.min("dist_km").alias("min_dist_km"), F.countDistinct("gkg_id").alias("n_articles"))
    )


def window_files(quakes, files, window_h=WINDOW_H):
    """id, files_ok = number of GDELT files with status "ok" in [time, time + window_h] (quakes with none are absent)."""
    ok_files = files.where(F.col("status") == "ok").select(F.col("time").alias("f_time"),
                                                          F.to_date("time").alias("day"))
    return (
        quakes.select("id", F.col("time").alias("q_time"))
        .withColumn("day", _days_after("q_time", window_h))
        .join(ok_files, "day")
        .where((F.col("f_time") >= F.col("q_time"))
               & (F.col("f_time") <= F.col("q_time") + F.expr(f"INTERVAL {window_h} HOURS")))
        .groupBy("id")
        .agg(F.count("*").alias("files_ok"))
    )


def aftershocks(quakes, window_h=WINDOW_H, radius_km=RADIUS_KM):
    """id of quakes with a quake of the same or larger magnitude within radius_km in the window_h before."""
    window = F.expr(f"INTERVAL {window_h} HOURS")
    earlier = quakes.select(F.col("time").alias("o_time"), F.col("mag").alias("o_mag"),
                            F.col("latitude").alias("o_lat"), F.col("longitude").alias("o_lon"),
                            F.to_date("time").alias("day"))
    return (
        quakes.select("id", F.col("time").alias("q_time"), F.col("mag").alias("q_mag"),
                      F.col("latitude").alias("q_lat"), F.col("longitude").alias("q_lon"))
        .withColumn("day", F.explode(F.sequence(F.date_sub(F.to_date("q_time"), window_h // 24),
                                                F.to_date("q_time"))))
        .join(earlier, "day")
        .where((F.col("o_time") < F.col("q_time")) & (F.col("o_time") >= F.col("q_time") - window))
        .where(F.col("o_mag") >= F.col("q_mag"))
        .where(distance_km(F.col("q_lat"), F.col("q_lon"), F.col("o_lat"), F.col("o_lon")) <= radius_km)
        .select("id").distinct()
    )


def coverage(quakes, articles, files, window_h=WINDOW_H, radius_km=RADIUS_KM, min_files=MIN_FILES):
    """quakes + n_articles, min_dist_km, files_ok, gdelt_complete, aftershock, covered.

    covered: True / False when gdelt_complete, NULL otherwise.
    min_dist_km is the distance of the nearest matching place (NULL if none), so a smaller radius r
    can be checked with min_dist_km <= r.
    """
    return (
        quakes
        .join(match_articles(quakes, articles, window_h, radius_km), "id", "left")
        .join(window_files(quakes, files, window_h), "id", "left")
        .join(aftershocks(quakes, window_h, radius_km).withColumn("aftershock", F.lit(True)), "id", "left")
        .na.fill(0, ["n_articles", "files_ok"])
        .na.fill(False, ["aftershock"])
        .withColumn("gdelt_complete", F.col("files_ok") >= min_files)
        .withColumn("covered", F.when(F.col("gdelt_complete"), F.col("n_articles") > 0))
    )
