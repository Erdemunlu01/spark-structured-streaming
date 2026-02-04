# Delta Lake Data Validation Script - Case Study 1 (English)

# Load Spark libraries
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("Delta Lake Checker")
    .master("yarn")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")  # Delta Lake jars
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Delta Lake path
delta_path = "hdfs://localhost:9000/user/train/delta-stream-caseStudy"

# Read Delta table
df_delta = spark.read.format("delta").load(delta_path)

# Display data
df_delta.show()

# Record count check
print(df_delta.count())
