# Delta Table Creation - Case Study 1 (English)

# Load Spark libraries
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("create delta lake")
    .master("yarn")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")  # Delta Lake jars
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Delta Lake library
from delta.tables import *

# Data schema definition
customer_schema = (
    "customerId int, "
    "customerFName string, "
    "customerLName string, "
    "customerCity string, "
    "time timestamp"
)

# Read batch data (data-generator output)
customer_batch_df = (
    spark.read
    .format("csv")
    .schema(customer_schema)
    .option("header", False)
    .load("file:///home/train/data-generator/output")
)

# Delta table path (HDFS)
deltaPath = "hdfs://localhost:9000/user/train/delta-stream-customers"

# Create Delta table and write batch data
customer_batch_df.write.format("delta").mode("overwrite").save(deltaPath)

# Read Delta table
delta_table = DeltaTable.forPath(spark, deltaPath)

# Display data for validation
delta_table.toDF().show()

# Record count check
print(delta_table.toDF().count())
