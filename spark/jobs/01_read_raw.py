
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("ChicagoCrimeReadRaw")
    .master("spark://spark_master:7077")
    .getOrCreate()
)

input_path = "hdfs://namenode:9000/crime/raw/Crimes_-_2001_to_Present.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("========== SCHEMA ==========")
df.printSchema()

print("========== ROW COUNT ==========")
print(df.count())

print("========== SAMPLE ==========")
df.show(10, truncate=False)

spark.stop()