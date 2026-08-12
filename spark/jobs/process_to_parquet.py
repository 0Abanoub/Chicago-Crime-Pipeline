import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofweek, when

def main():
    # 1. إنشاء الـ Spark Session
    spark = SparkSession.builder \
        .appName("ChicagoCrime_ProcessToParquet") \
        .getOrCreate()

    # تقليل الـ Logs غير الضرورية في الـ Terminal
    spark.sparkContext.setLogLevel("WARN")

    print("=== Starting PySpark Parquet Processing Job ===")

    # 2. مسارات الملفات في HDFS
    # يمكنك ضبط المسار حسب المكان الذي يتواجد به ملف الـ CSV الخطي في HDFS
    input_hdfs_path = "hdfs://namenode:9000/raw/Chicago_Crime_Data.csv"
    output_parquet_path = "hdfs://namenode:9000/processed/chicago_crime_parquet"

    # 3. قراءة ملف الـ CSV مع استنتاج الـ Schema تلقائيًا
    print(f"Reading raw data from: {input_hdfs_path}")
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_hdfs_path)

    # 4. تنظيف ومعالجة البيانات الأساسية
    print("Cleaning data and generating partition columns...")
    
    # تحويل عامود Date لنوع Timestamp صريح وتمرير الشهور والسنوات كأعمدة جديدة للـ Partitioning
    df_cleaned = df_raw \
        .withColumn("ParsedDate", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")) \
        .withColumn("Year_Partition", year(col("ParsedDate"))) \
        .withColumn("Month_Partition", month(col("ParsedDate"))) \
        .dropna(subset=["ID", "Date", "Primary Type"]) \
        .dropDuplicates(["ID"])

    # 5. كتابة البيانات بتنسيق Parquet في HDFS مع التقسيم (Partitioning)
    print(f"Writing dataset as Parquet to: {output_parquet_path}")
    
    df_cleaned.write \
        .mode("overwrite") \
        .partitionBy("Year_Partition") \
        .parquet(output_parquet_path)

    print("=== Parquet Processing Completed Successfully! ===")

    # 6. قراءة عينة من ملفات الـ Parquet للتأكد من نجاح الكتابة والقراءة
    print("\nReading sample from created Parquet files:")
    df_parquet = spark.read.parquet(output_parquet_path)
    
    print(f"Total rows in Parquet: {df_parquet.count()}")
    df_parquet.select("ID", "ParsedDate", "Primary Type", "Arrest", "Year_Partition").show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()