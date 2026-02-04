# Spark Structured Streaming - Case Study 1 (English)

# Load Spark libraries
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("case study io 1")
    .master("yarn")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")  # Delta Lake jars
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Delta Lake library
from delta.tables import *

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

# PostgreSQL connection string
jdbcUrl = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"

# Delta Lake path
deltaPath = "hdfs://localhost:9000/user/train/delta-stream-caseStudy"

# Hive path (table name is provided via saveAsTable)
hivePath = "hdfs://localhost:9000/user/train/hive/hive-stream-caseStudy"

# foreachBatch function
# Routes records to different sinks based on sensor ID
def write_multiple(df, batchid):
    df.show()

    # Filter by sensor ID
    pg_df = df.filter(F.col("device") == "00:0f:00:70:91:0a")
    hive_df = df.filter(F.col("device") == "b8:27:eb:bf:9d:51")
    delta_df = df.filter(F.col("device") == "1c:bf:ce:15:ec:4d")

    if pg_df.take(1):
        # Write to PostgreSQL
        pg_df.write.jdbc(
            url=jdbcUrl,
            table="iot_stream",
            mode="append",
            properties={"driver": "org.postgresql.Driver"}
        )

    if hive_df.take(1):
        # Write to Hive table
        hive_df.write.mode("append").format("parquet").saveAsTable("test1.case_study_io")

    if delta_df.take(1):
        # Write to Delta Lake
        delta_df.write.format("delta").mode("append").save(deltaPath)

# Checkpoint directory
checkpoint_path = "hdfs://localhost:9000/user/train/checkpoints/case_study_v1"

# Start streaming query
streamingQuery = (
    lines2.writeStream
    .foreachBatch(write_multiple)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

# Keep the streaming query running
streamingQuery.awaitTermination()
