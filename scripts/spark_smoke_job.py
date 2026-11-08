"""Spark smoke test: HDFS write/read, Hive metastore and Kafka connector."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").enableHiveSupport().getOrCreate()

path = "hdfs://namenode:9000/tmp/smoke_parquet"
spark.range(100).write.mode("overwrite").parquet(path)
assert spark.read.parquet(path).count() == 100, "HDFS round trip failed"

spark.sql("CREATE DATABASE IF NOT EXISTS smoke")
spark.sql("DROP TABLE IF EXISTS smoke.t")
spark.sql(f"CREATE EXTERNAL TABLE smoke.t (id BIGINT) STORED AS PARQUET LOCATION '{path}'")
assert spark.sql("SELECT count(*) FROM smoke.t").first()[0] == 100, "Hive table read failed"
spark.sql("DROP TABLE smoke.t")
spark.sql("DROP DATABASE smoke")

kafka_rows = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "usgs")
    .option("startingOffsets", "earliest")
    .load()
    .count()
)
print(f"SMOKE OK (rows in Kafka topic usgs: {kafka_rows})")
spark.stop()
