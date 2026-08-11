from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp

def main():
    # 1. إنشاء Spark Session
    spark = SparkSession.builder \
        .appName("Chicago Crime Data Processing") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. قراءة البيانات الخام من HDFS
    hdfs_input_path = "hdfs://namenode:9000/raw/Chicago_Crime_Data.csv"
    print(f"--> Reading raw data from: {hdfs_input_path}")
    df_raw = spark.read.csv(hdfs_input_path, header=True, inferSchema=True)

    # 3. معالجة البيانات وتنظيفها (Cleansing & Transformation)
    df_cleaned = df_raw \
        .dropDuplicates(["ID"]) \
        .filter(col("ID").isNotNull()) \
        .withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")) \
        .withColumn("processed_at", current_timestamp())

    # 4. فواحص الجودة (Data Quality Check)
    total_records = df_cleaned.count()
    print(f"--> Data Quality Check Passed. Total Valid Records: {total_records}")

    # 5. حفظ البيانات المعالجة بصيغة Parquet على HDFS
    hdfs_output_path = "hdfs://namenode:9000/processed/chicago_crime_parquet"
    print(f"--> Saving processed Parquet data to: {hdfs_output_path}")
    
    df_cleaned.write \
        .mode("overwrite") \
        .parquet(hdfs_output_path)

    print("--> Job Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()