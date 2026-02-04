# Spark Structured Streaming - GroupBy Window Aggregation (Case Study 2 - English)

# Load Spark libraries
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("from csv groupby window")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Data schema (iris-like streaming dataset example)
iris_schema = (
    "row_id int, "
    "SepalLengthCm float, "
    "SepalWidthCm float, "
    "PetalLengthCm float, "
    "PetalWidthhCm float, "
    "Species string, "
    "time timestamp"
)

# Read streaming data (data-generator output)
lines = (
    spark.readStream
    .format("csv")
    .schema(iris_schema)
    .option("header", False)
    .option("maxFilesPerTrigger", 1)
    .load("file:///home/train/data-generator/output")
)

# Window + groupBy aggregation (stateful operation)
lines2 = (
    lines
    .groupBy(
        F.window(F.col("time"), "2 minutes", "1 minutes"),
        F.col("Species")
    )
    .count()
)

# Write results to console sink
streamingQuery = (
    lines2.writeStream
    .format("console")
    .outputMode("complete")  # complete mode for stateful aggregations
    .trigger(processingTime="2 second")
    .option("checkpointLocation", "file:///home/train/checkpoint/checkpoint-grby-windows")
    .option("numRows", 4)
    .option("truncate", False)
    .start()
)

# Keep the streaming query running
streamingQuery.awaitTermination()
