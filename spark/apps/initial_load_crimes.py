"""
Chicago Crime — One-time initial load: CSV -> partitioned Parquet source
========================================================================
Reads the full Chicago Crime CSV (already uploaded to HDFS /landing) once and
writes a partitioned Parquet dataset under /source/crimes:

    /source/crimes/
        year=2001/
            month=1/
            month=2/
            ...
        year=2002/
            ...

Partitioning:
  year, month  -> derived from the parsed crime "Date" column (NOT the "Year"
                  column, which is a data column that can disagree with Date —
                  that disagreement is exactly what the YEAR_MISMATCH DQ rule
                  flags later).
  Rows whose Date cannot be parsed cannot be assigned a month, so they are
  quarantined into the sentinel partition year=0/month=0 with a
  `_source_error` column so nothing is silently dropped. Monthly batches never
  read that sentinel partition (partition pruning excludes it).

This job runs ONCE (or on demand to rebuild the source lake). It must NEVER be
part of the monthly DAG — the monthly DAG reads only one year=YYYY/month=M
partition.

Idempotency: writes use overwrite mode, so re-running the job rebuilds the
entire source dataset. It is a full rebuild by design (one-time operation).

Run via the chicago_crime_initial_load DAG, or directly:

    spark-submit --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-apps/initial_load_crimes.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, month, to_timestamp, when, year
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

landing_path = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000/landing/Crimes_-_2001_to_Present.csv"
source_path = "hdfs://namenode:9000/source/crimes"

# Same schema enforcement as process_crimes.py (mirrors the 22 CSV columns)
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

spark = (
    SparkSession.builder
    .appName("ChicagoCrimeInitialLoad")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# Dynamic partition overwrite: the two writes below only ever replace the
# partitions present in their own dataframe, so the sentinel partition write
# cannot wipe the valid months (and vice versa).
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df = spark.read.option("header", True).csv(landing_path, schema=schema)
row_count_total = df.count()
print(f"[initial-load] rows read from CSV: {row_count_total}")

# ---------------------------------------------------------------------------
# Derive year/month from the parsed crime date. Unparseable dates go to the
# sentinel partition (year=0, month=0) with a _source_error marker.
# ---------------------------------------------------------------------------
loaded = df.withColumn(
    "_parsed_date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")
)

valid = loaded.filter(col("_parsed_date").isNotNull()).drop("_parsed_date")
quarantined = (
    loaded.filter(col("_parsed_date").isNull())
    .drop("_parsed_date")
    .withColumn("_source_error", lit("INVALID_DATE"))
)

# NOTE: the data column `Year` is renamed to `YearReported` before the
# partition columns are added. Spark treats `Year` and the `year` partition
# column as the same column case-insensitively and would silently DROP the data
# column (exactly what happened to the `Year` column in the old curated write).
# process_crimes_partitioned.py restores the name to `Year` after reading.
valid_partitioned = (
    valid
    .withColumnRenamed("Year", "YearReported")
    .withColumn("year", year(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
    .withColumn("month", month(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
)

quarantine = quarantined.withColumn("year", lit(0)).withColumn("month", lit(0))

row_count_valid = valid.count()
row_count_quarantined = quarantined.count()
print(f"[initial-load] valid rows (partitionable): {row_count_valid}")
print(f"[initial-load] quarantined rows (INVALID_DATE -> year=0/month=0): {row_count_quarantined}")
print(f"[initial-load] total = valid + quarantined: {row_count_total} = {row_count_valid + row_count_quarantined}")

# Overwrite rebuilds the whole source dataset (one-time operation). Dynamic
# partition overwrite keeps the sentinel write scoped to year=0/month=0.
valid_partitioned.write.mode("overwrite").partitionBy("year", "month").parquet(source_path)
if row_count_quarantined > 0:
    quarantine.write.mode("overwrite").partitionBy("year", "month").parquet(source_path)
else:
    print("[initial-load] no quarantined rows, sentinel partition not created")

print(f"[initial-load] wrote partitioned source to {source_path}")

partitions = spark.read.parquet(source_path).select("year", "month").distinct().orderBy("year", "month")
print("[initial-load] source partitions:")
partitions.show(200, truncate=False)

spark.stop()
