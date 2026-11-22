from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LOCAL = "file:///opt/app/data/raw"

# GKG locations: blocks separated by ';', fields by '#': type#name#country#adm1#lat#lon#feature_id
LOCATIONS = """
    transform(
        filter(split(locations, ';'), x -> x != ''),
        x -> named_struct(
            'name', split(x, '#')[1],
            'country', split(x, '#')[2],
            'latitude', cast(split(x, '#')[4] as double),
            'longitude', cast(split(x, '#')[5] as double)))
"""


def usgs_table(raw):
    return raw.select(
        "id",
        F.timestamp_millis("properties.time").alias("time"),
        F.timestamp_millis("properties.updated").alias("updated"),
        F.col("properties.mag").cast("double").alias("mag"),
        F.col("properties.magType").alias("mag_type"),
        F.col("properties.place").alias("place"),
        F.col("geometry.coordinates")[1].cast("double").alias("latitude"),
        F.col("geometry.coordinates")[0].cast("double").alias("longitude"),
        F.col("geometry.coordinates")[2].cast("double").alias("depth_km"),
        F.col("properties.type").alias("type"),
        F.col("properties.status").alias("status"),
        F.col("properties.net").alias("net"),
        F.col("properties.sig").cast("int").alias("sig"),
        F.col("properties.tsunami").cast("int").alias("tsunami"),
        F.col("properties.felt").cast("int").alias("felt"),
        F.col("properties.alert").alias("alert"),
    )


def gdelt_table(raw):
    return raw.select(
        "gkg_id",
        F.to_timestamp("date", "yyyyMMddHHmmss").alias("time"),
        "source",
        "url",
        "themes",
        F.expr(LOCATIONS).alias("locations"),
    )


def files_table(raw):
    return raw.select(
        F.to_timestamp("file", "yyyyMMddHHmmss").alias("time"),
        "status",
        F.col("articles").cast("int").alias("articles"),
    )


def save(spark, df, table):
    path = f"/history/{table}"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema)
    spark.sql(f"DROP TABLE IF EXISTS seismic.{table}")
    spark.sql(f"CREATE EXTERNAL TABLE seismic.{table} ({columns}) STORED AS PARQUET LOCATION '{path}'")
    print(f"seismic.{table}: {spark.table(f'seismic.{table}').count()} rows")


def main():
    spark = (
        SparkSession.builder.appName("load-history")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS seismic")
    save(spark, usgs_table(spark.read.json(f"{LOCAL}/usgs_history/*.jsonl")), "usgs_history")
    save(spark, gdelt_table(spark.read.json(f"{LOCAL}/gdelt_history/articles/*.jsonl")), "gdelt_history")
    save(spark, files_table(spark.read.csv(f"{LOCAL}/gdelt_history/files/*.csv", header=True)), "gdelt_files")
    spark.stop()


if __name__ == "__main__":
    main()
