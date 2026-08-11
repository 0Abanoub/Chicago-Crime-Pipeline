"""
Lab: Batch processing with Spark + HDFS
----------------------------------------
Reads a text file from HDFS, counts word frequency, writes result back to HDFS.

Setup (run once, from your host terminal):
    docker exec -it namenode hdfs dfs -mkdir -p /labs/batch/input
    echo "spark is great spark is fast hdfs is reliable" > /tmp/sample.txt
    docker cp /tmp/sample.txt namenode:/tmp/sample.txt
    docker exec -it namenode hdfs dfs -put /tmp/sample.txt /labs/batch/input/sample.txt

Run:
    docker exec -it spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/batch_wordcount.py

Check the output:
    docker exec -it namenode hdfs dfs -cat /labs/batch/output/part-*
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

INPUT_PATH = "hdfs://namenode:9000/labs/batch/input/sample.txt"
OUTPUT_PATH = "hdfs://namenode:9000/labs/batch/output"

def main():
    spark = (
        SparkSession.builder
        .appName("BatchWordCount")
        .getOrCreate()
    )

    df = spark.read.text(INPUT_PATH)
    words = df.select(explode(split(col("value"), r"\s+")).alias("word"))
    counts = words.groupBy("word").count().orderBy(col("count").desc())

    counts.show(truncate=False)

    (
        counts.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )
    print(f"Wrote results to {OUTPUT_PATH}")

    spark.stop()

if __name__ == "__main__":
    main()
