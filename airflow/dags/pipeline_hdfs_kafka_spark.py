"""
Lab: End-to-end pipeline orchestrated by Airflow
--------------------------------------------------
Ties together HDFS, Kafka and Spark in one DAG so students can see how a
real pipeline is scheduled and monitored, not just how each tool works alone.

Flow:
  1. ensure_hdfs_dirs   -> create HDFS folders for this run
  2. produce_kafka_events -> push a handful of JSON events to Kafka via kafka-python
  3. spark_batch_job    -> a PySpark job (run in-process via the pyspark pip
                           package installed in the Airflow image) that reads
                           from HDFS and writes a summary back to HDFS

This intentionally avoids SparkSubmitOperator/DockerOperator so it works
out of the box without extra Airflow connections - good enough for teaching.
Once students are comfortable, point them at SparkSubmitOperator or
DockerOperator as a "level up" exercise.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "instructor",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def ensure_hdfs_dirs():
    from hdfs import InsecureClient

    client = InsecureClient("http://namenode:9870", user="root")
    for path in ["/labs/pipeline/input", "/labs/pipeline/output"]:
        client.makedirs(path)
    print("HDFS directories ready")


def produce_kafka_events():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    events = [
        {"user_id": i, "action": "purchase", "amount": 10 + i, "ts": datetime.utcnow().isoformat()}
        for i in range(1, 6)
    ]
    for event in events:
        producer.send("pipeline-events", event)
    producer.flush()
    print(f"Sent {len(events)} events to topic 'pipeline-events'")


def spark_batch_job():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("AirflowTriggeredBatchJob")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    data = [("purchase", 5), ("click", 12), ("view", 30)]
    df = spark.createDataFrame(data, ["action", "count"])
    df.show()

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        "hdfs://namenode:9000/labs/pipeline/output"
    )
    spark.stop()
    print("Spark batch job complete, results in HDFS at /labs/pipeline/output")


with DAG(
    dag_id="pipeline_hdfs_kafka_spark",
    description="End-to-end lab: HDFS + Kafka + Spark orchestrated by Airflow",
    default_args=default_args,
    schedule=None,  # trigger manually for labs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "integration"],
) as dag:

    t1 = PythonOperator(task_id="ensure_hdfs_dirs", python_callable=ensure_hdfs_dirs)
    t2 = PythonOperator(task_id="produce_kafka_events", python_callable=produce_kafka_events)
    t3 = PythonOperator(task_id="spark_batch_job", python_callable=spark_batch_job)

    t1 >> t2 >> t3
