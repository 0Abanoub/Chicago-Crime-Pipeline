"""
Chicago Crime — Spark processing + data quality
================================================
Reads ONE monthly raw batch (CSV) from HDFS and writes:

  /curated/year=YYYY/month=M/   valid records as Parquet (year/month partitioned)
  /rejected/<batch_month>/      records that failed a row-level data-quality check,
                                with the failing checks in `reject_reason`

Data-quality checks applied (row level):
  ID_NULL, PRIMARY_TYPE_NULL, INVALID_DATE, YEAR_MISMATCH,
  NULL_GEO, INVALID_LAT, INVALID_LON, DUPLICATE_ID

Idempotency: curated uses dynamic partition overwrite, so rerunning a batch
only replaces ITS OWN year/month partition and never touches other months.
Rejected writes to the batch-specific directory /rejected/<batch_month>/, so
reruns replace only that batch's rejects.

Run via the Airflow DAG (chicago_crime_pipeline) with one argument = batch_month
(e.g. "2001-01"), or directly against the cluster:

    spark-submit --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-apps/process_crimes.py 2001-01
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, row_number, to_timestamp, when, year
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

batch_month = sys.argv[1] if len(sys.argv) > 1 else "2001-01"
batch_year, batch_month_num = (int(x) for x in batch_month.split("-"))

spark = (
    SparkSession.builder
    .appName(f"ChicagoCrimeProcessing_{batch_month}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# Dynamic partition overwrite: only the partitions present in this dataframe
# (this batch's year/month) are replaced, all other months stay untouched.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ---------------------------------------------------------------------------
# 1. Schema enforcement — mirrors the 22 columns of the Chicago crime dataset
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Case Number", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Location Description", StringType(), True),
    StructField("Arrest", BooleanType(), True),
    StructField("Domestic", BooleanType(), True),
    StructField("Beat", IntegerType(), True),
    StructField("District", IntegerType(), True),
    StructField("Ward", IntegerType(), True),
    StructField("Community Area", IntegerType(), True),
    StructField("FBI Code", StringType(), True),
    StructField("X Coordinate", IntegerType(), True),
    StructField("Y Coordinate", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Updated On", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Location", StringType(), True),
])

raw_path = f"hdfs://namenode:9000/raw/{batch_month}/crime.csv"
curated_path = "hdfs://namenode:9000/curated"
rejected_path = f"hdfs://namenode:9000/rejected/{batch_month}"

df = spark.read.option("header", True).csv(raw_path, schema=schema)
row_count_raw = df.count()
print(f"[reconciliation] row_count_raw = {row_count_raw}")

# ---------------------------------------------------------------------------
# 2. Row-level data-quality checks
# ---------------------------------------------------------------------------
checked = df.withColumn("reject_reason", lit(None).cast("string"))

# ID must not be null
checked = checked.withColumn(
    "reject_reason",
    when(col("ID").isNull(), concat_ws(";", col("reject_reason"), lit("ID_NULL")))
    .otherwise(col("reject_reason")),
)

# Primary Type must be present
checked = checked.withColumn(
    "reject_reason",
    when(col("Primary Type").isNull(), concat_ws(";", col("reject_reason"), lit("PRIMARY_TYPE_NULL")))
    .otherwise(col("reject_reason")),
)

# Date must parse, and its year must agree with the Year column
checked = checked.withColumn("parsed_date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
checked = checked.withColumn(
    "reject_reason",
    when(col("parsed_date").isNull(), concat_ws(";", col("reject_reason"), lit("INVALID_DATE")))
    .when(year(col("parsed_date")) != col("Year"), concat_ws(";", col("reject_reason"), lit("YEAR_MISMATCH")))
    .otherwise(col("reject_reason")),
)

# Coordinates must exist and fall inside Chicago's bounding box
checked = checked.withColumn(
    "reject_reason",
    when(
        col("Latitude").isNull() | col("Longitude").isNull(),
        concat_ws(";", col("reject_reason"), lit("NULL_GEO")),
    )
    .when(
        (col("Latitude") < 41.0) | (col("Latitude") > 43.0),
        concat_ws(";", col("reject_reason"), lit("INVALID_LAT")),
    )
    .when(
        (col("Longitude") < -88.5) | (col("Longitude") > -87.0),
        concat_ws(";", col("reject_reason"), lit("INVALID_LON")),
    )
    .otherwise(col("reject_reason")),
)

print(
    "Data-quality checks applied: ID_NULL, PRIMARY_TYPE_NULL, INVALID_DATE, "
    "YEAR_MISMATCH, NULL_GEO, INVALID_LAT, INVALID_LON, DUPLICATE_ID"
)

# ---------------------------------------------------------------------------
# 3. Split valid / invalid, then de-duplicate by ID
# ---------------------------------------------------------------------------
invalid_records = checked.filter(col("reject_reason").isNotNull()).drop("parsed_date")
valid_records = checked.filter(col("reject_reason").isNull()).drop("parsed_date", "reject_reason")

window = Window.partitionBy("ID").orderBy("Date")
valid_ranked = valid_records.withColumn("row_number", row_number().over(window))

curated = valid_ranked.filter(col("row_number") == 1).drop("row_number")
duplicates = (
    valid_ranked.filter(col("row_number") > 1)
    .withColumn("reject_reason", lit("DUPLICATE_ID"))
    .drop("row_number")
)

rejected = invalid_records.unionByName(duplicates)

row_count_curated = curated.count()
row_count_rejected = rejected.count()
print(f"[reconciliation] row_count_curated = {row_count_curated}")
print(f"[reconciliation] row_count_rejected = {row_count_rejected}")
print(f"[reconciliation] raw = curated + rejected: {row_count_raw} = {row_count_curated + row_count_rejected}")

# ---------------------------------------------------------------------------
# 4. Write curated Parquet (partitioned by year + month, dynamic overwrite so
#    only THIS batch's partitions are replaced) and batch-scoped rejected
# ---------------------------------------------------------------------------
# All valid rows belong to this batch month (ingest filtered by the crime
# date and YEAR_MISMATCH is a reject rule), so the partitions derive directly
# from the batch:
#   year  = batch_year, month = batch_month
# The redundant `Year` data column is dropped to avoid a case-insensitive
# column clash with the `year` partition column in the Hive table (Spark
# treats `Year` and `year` as the same column, so we drop it first and then
# add the partition columns with `lit`, which cannot collide).
curated = (
    curated
    .drop("Year")
    .withColumn("year", lit(batch_year))
    .withColumn("month", lit(batch_month_num))
)

curated.write.mode("overwrite").partitionBy("year", "month").parquet(curated_path)
rejected.write.mode("overwrite").parquet(rejected_path)

print(f"Batch {batch_month} processed: curated -> {curated_path}, rejected -> {rejected_path}")
spark.stop()
