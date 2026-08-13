"""
Chicago Crime — Monthly Spark processing + data quality (partitioned source)
=============================================================================
Reads ONLY the requested month's Parquet partition from the pre-built source
lake and writes:

  /curated/year=YYYY/month=M/   valid records as Parquet (year/month partitioned)
  /rejected/<batch_month>/      records that failed a row-level data-quality check,
                                with the failing checks in `reject_reason`

The source is read by exact partition path:

  hdfs://namenode:9000/source/crimes/year=YYYY/month=M

so Spark reads only that partition — the 1.85 GB CSV and all other months are
NEVER scanned during a monthly run.

Data-quality checks applied (row level, identical to process_crimes.py):
  ID_NULL, PRIMARY_TYPE_NULL, INVALID_DATE, YEAR_MISMATCH,
  NULL_GEO, INVALID_LAT, INVALID_LON, DUPLICATE_ID

Idempotency: curated uses dynamic partition overwrite, so rerunning a batch
only replaces ITS OWN year/month partition and never touches other months.
Rejected writes to the batch-specific directory /rejected/<batch_month>/, so
reruns replace only that batch's rejects.

Optional --explain: prints the Spark physical plan for a filtered read of the
whole source tree, demonstrating that the partition predicate is pushed down
(PartitionFilters) and no other month is scanned. The real read still uses the
exact partition path.

Run via the Airflow DAG (chicago_crime_pipeline_v2) with batch_month as the
only argument, or directly:

    spark-submit --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-apps/process_crimes_partitioned.py 2001-01 [--explain]
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, row_number, to_timestamp, when, year
from pyspark.sql.window import Window

batch_month = sys.argv[1] if len(sys.argv) > 1 else "2001-01"
explain = "--explain" in sys.argv
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
# Read ONLY the requested partition (partition pruning by construction)
# ---------------------------------------------------------------------------
source_root = "hdfs://namenode:9000/source/crimes"
source_partition = f"{source_root}/year={batch_year}/month={batch_month_num}"

# The partition columns come along with the read; they are redundant for DQ
# (the batch month is known) and would collide with the curated partition
# columns later, so drop them immediately after reading. The source lake keeps
# the original `Year` data column under the name `YearReported` (to avoid the
# case-insensitive clash with the `year` partition column during the initial
# load) — restore the original name so the DQ rules stay identical to
# process_crimes.py.
df = spark.read.parquet(source_partition).drop("year", "month").withColumnRenamed("YearReported", "Year")
row_count_raw = df.count()
print(f"[reconciliation] batch_month = {batch_month}")
print(f"[reconciliation] source partition read = {source_partition}")
print(f"[reconciliation] row_count_raw = {row_count_raw}")

if explain:
    print("[explain] Physical plan for a filtered read of the WHOLE source tree:")
    (
        spark.read.parquet(source_root)
        .filter((col("year") == batch_year) & (col("month") == batch_month_num))
        .explain(extended=False)
    )

# ---------------------------------------------------------------------------
# 2. Row-level data-quality checks (same rules as process_crimes.py)
# ---------------------------------------------------------------------------
checked = df.withColumn("reject_reason", lit(None).cast("string"))

checked = checked.withColumn(
    "reject_reason",
    when(col("ID").isNull(), concat_ws(";", col("reject_reason"), lit("ID_NULL")))
    .otherwise(col("reject_reason")),
)

checked = checked.withColumn(
    "reject_reason",
    when(col("Primary Type").isNull(), concat_ws(";", col("reject_reason"), lit("PRIMARY_TYPE_NULL")))
    .otherwise(col("reject_reason")),
)

checked = checked.withColumn("parsed_date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
checked = checked.withColumn(
    "reject_reason",
    when(col("parsed_date").isNull(), concat_ws(";", col("reject_reason"), lit("INVALID_DATE")))
    .when(year(col("parsed_date")) != col("Year"), concat_ws(";", col("reject_reason"), lit("YEAR_MISMATCH")))
    .otherwise(col("reject_reason")),
)

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
#    only THIS batch's partitions are replaced) and batch-scoped rejected.
#    The redundant `Year` data column is dropped to avoid a case-insensitive
#    clash with the `year` partition column (see process_crimes.py).
# ---------------------------------------------------------------------------
curated_path = "hdfs://namenode:9000/curated"
rejected_path = f"hdfs://namenode:9000/rejected/{batch_month}"

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
