"""
Lab: Spark SQL
----------------
Loads a CSV from HDFS, registers it as a temp view, and runs SQL queries.

Setup: upload any CSV with a header to HDFS first, e.g.:
    docker exec -it namenode hdfs dfs -mkdir -p /labs/sql/input
    docker cp your_data.csv namenode:/tmp/your_data.csv
    docker exec -it namenode hdfs dfs -put /tmp/your_data.csv /labs/sql/input/

Edit INPUT_PATH below to match your file, then run:
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/spark_sql_demo.py
"""
from pyspark.sql import SparkSession

INPUT_PATH = "hdfs://namenode:9000/labs/sql/input/*.csv"

def main():
    spark = (
        SparkSession.builder
        .appName("SparkSQLDemo")
        .getOrCreate()
    )

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    df.createOrReplaceTempView("data")

    print("Schema:")
    df.printSchema()

    print("Row count:")
    spark.sql("SELECT COUNT(*) AS total_rows FROM data").show()

    print("Preview:")
    spark.sql("SELECT * FROM data LIMIT 10").show()

    # Replace with real column names for your dataset in class
    # spark.sql("SELECT category, COUNT(*) FROM data GROUP BY category ORDER BY 2 DESC").show()

    spark.stop()

if __name__ == "__main__":
    main()
