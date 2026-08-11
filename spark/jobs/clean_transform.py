from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek,
    when,
    trim,
    upper
)

# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("ChicagoCrime-CleanTransform")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "14")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Paths inside Docker
# ============================================================

input_path = "/opt/project/data/raw/chicago_crime.csv"
output_path = "/opt/project/data/processed/chicago_crime_clean"


# ============================================================
# 3. Read Raw CSV
# ============================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("=" * 60)
print("RAW DATA")
print("=" * 60)

print("Rows:", df.count())


# ============================================================
# 4. Rename Columns
# ============================================================

df = (
    df
    .withColumnRenamed("Case Number", "Case_Number")
    .withColumnRenamed("Primary Type", "Primary_Type")
    .withColumnRenamed("Location Description", "Location_Description")
    .withColumnRenamed("Community Area", "Community_Area")
    .withColumnRenamed("FBI Code", "FBI_Code")
    .withColumnRenamed("X Coordinate", "X_Coordinate")
    .withColumnRenamed("Y Coordinate", "Y_Coordinate")
    .withColumnRenamed("Updated On", "Updated_On")
)


# ============================================================
# 5. Clean String Columns
# ============================================================

string_columns = [
    "Block",
    "IUCR",
    "Primary_Type",
    "Description",
    "Location_Description",
    "FBI_Code"
]

for c in string_columns:
    df = df.withColumn(
        c,
        trim(col(c))
    )


# ============================================================
# 6. Convert Date Columns
# ============================================================

df = (
    df
    .withColumn(
        "Date",
        to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")
    )
    .withColumn(
        "Updated_On",
        to_timestamp(col("Updated_On"), "MM/dd/yyyy hh:mm:ss a")
    )
)


# ============================================================
# 7. Create Time Features
# ============================================================

df = (
    df
    .withColumn("Crime_Year", year(col("Date")))
    .withColumn("Crime_Month", month(col("Date")))
    .withColumn("Crime_Day", dayofmonth(col("Date")))
    .withColumn("Crime_Hour", hour(col("Date")))
    .withColumn("Day_Of_Week", dayofweek(col("Date")))
    .withColumn(
        "Is_Weekend",
        when(col("Day_Of_Week").isin(1, 7), True)
        .otherwise(False)
    )
)


# ============================================================
# 8. Remove Invalid Records
# ============================================================

df_clean = (
    df
    .filter(col("ID").isNotNull())
    .filter(col("Date").isNotNull())
    .filter(col("Primary_Type").isNotNull())
)


# ============================================================
# 9. Show Schema
# ============================================================

print("=" * 60)
print("CLEAN DATA SCHEMA")
print("=" * 60)

df_clean.printSchema()


# ============================================================
# 10. Show Sample
# ============================================================

print("=" * 60)
print("CLEAN DATA SAMPLE")
print("=" * 60)

df_clean.select(
    "ID",
    "Case_Number",
    "Date",
    "Primary_Type",
    "Crime_Year",
    "Crime_Month",
    "Crime_Hour",
    "Day_Of_Week",
    "Is_Weekend"
).show(10, truncate=False)


# ============================================================
# 11. Count Clean Rows
# ============================================================

clean_count = df_clean.count()

print("=" * 60)
print("CLEAN ROW COUNT")
print("=" * 60)

print("Clean Rows:", clean_count)


# ============================================================
# 12. Crime Types
# ============================================================

print("=" * 60)
print("CRIME TYPES")
print("=" * 60)

(
    df_clean
    .groupBy("Primary_Type")
    .count()
    .orderBy(col("count").desc())
    .show(20, truncate=False)
)


# ============================================================
# 13. Crimes By Year
# ============================================================

print("=" * 60)
print("CRIMES BY YEAR")
print("=" * 60)

(
    df_clean
    .groupBy("Crime_Year")
    .count()
    .orderBy("Crime_Year")
    .show(100)
)


# ============================================================
# 14. Crimes By Hour
# ============================================================

print("=" * 60)
print("CRIMES BY HOUR")
print("=" * 60)

(
    df_clean
    .groupBy("Crime_Hour")
    .count()
    .orderBy("Crime_Hour")
    .show(24)
)


# ============================================================
# 15. Arrest Statistics
# ============================================================

print("=" * 60)
print("ARREST STATISTICS")
print("=" * 60)

(
    df_clean
    .groupBy("Arrest")
    .count()
    .orderBy("Arrest")
    .show()
)


# ============================================================
# 16. Domestic Statistics
# ============================================================

print("=" * 60)
print("DOMESTIC STATISTICS")
print("=" * 60)

(
    df_clean
    .groupBy("Domestic")
    .count()
    .orderBy("Domestic")
    .show()
)


# ============================================================
# 17. Write Parquet
# ============================================================

print("=" * 60)
print("WRITING PARQUET")
print("=" * 60)

(
    df_clean
    .repartition(14)
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(output_path)
)

print("=" * 60)
print("CLEANING SUCCESS")
print("=" * 60)

print("Total Clean Rows:", clean_count)
print("Parquet Output:", output_path)


# ============================================================
# 18. Stop Spark
# ============================================================

spark.stop()