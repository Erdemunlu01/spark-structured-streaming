# Spark Structured Streaming - Windowed Aggregation (Case Study 2 - English)

# Load Spark libraries
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("from csv groupby window cs io")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# IoT data schema
data_schema = (
    "ts string, device string, co float, humidity float, "
    "light string, lpg float, motion string, smoke float, temp double"
)

# Read streaming data (data-generator output)
lines = (
    spark.readStream
    .format("csv")
    .schema(data_schema)
    .option("header", False)
    .option("maxFilesPerTrigger", 1)
    .load("file:///home/train/data-generator/output")
)

# Event time transformation (stateless operation)
lines2 = (
    lines
    .withColumn("ts_double", F.col("ts").cast("double"))
    .withColumn(
        "time",
        F.to_timestamp(F.from_unixtime(F.col("ts_double").cast("long")))
    )
    .drop("ts_double")
)

# Window aggregation (stateful operation)
lines3 = (
    lines2
    .groupBy(
        F.window(F.col("time"), "10 minutes", "5 minutes"),
        F.col("device")
    )
    .agg(
        F.count("*").alias("signal_count"),
        F.avg("co").alias("avg_co"),
        F.avg("humidity").alias("avg_humidity")
    )
)

# Write results to console sink
streamingQuery = (
    lines3.writeStream
    .format("console")
    .outputMode("update")  # update/complete for aggregations
    .option("truncate", False)
    .start()
)

# Keep the streaming query running
streamingQuery.awaitTermination()
