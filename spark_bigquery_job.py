from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

# Initialize SparkSession with BigQuery support
spark = (
    SparkSession.builder.appName("Load CSVs from GCS to BigQuery with Partitioning")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0",
    )
    .getOrCreate()
)

# GCS path to source CSVs
gcs_path = "gs://time_data01/*.csv"

# Read CSV files with headers and infer schema
df = spark.read.option("header", "true").option("inferSchema", "true").csv(gcs_path)

# Add today's date as a partition column
df = df.withColumn("load_date", current_date())

# Write to BigQuery with partitioning
df.write.format("bigquery").option(
    "table", "river-lane-453009-h7.dataproc_dataset.time_data"
).option("temporaryGcsBucket", "punch_data_hsbc").option(
    "partitionField", "load_date"
).option(
    "partitionType", "DAY"
).option(
    "createDisposition", "CREATE_IF_NEEDED"
).option(
    "writeDisposition", "WRITE_APPEND"
).save()
