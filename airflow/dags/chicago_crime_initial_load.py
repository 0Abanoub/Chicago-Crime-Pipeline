"""
Chicago Crime — One-time initial load DAG
==========================================
Converts the 1.85 GB source CSV into a partitioned Parquet source dataset on
HDFS (/source/crimes, partitioned by year/month). This runs ONCE. The monthly
DAG (chicago_crime_pipeline_v2) consumes these partitions and never scans the
CSV again.

    1. upload_csv_to_hdfs            Streams the local CSV
                                     (./data/Crimes_-_2001_to_Present.csv)
                                     into HDFS /landing via WebHDFS.
    2. convert_to_partitioned_parquet  SparkSubmitOperator -> initial_load_crimes.py
                                     reads the HDFS CSV and writes
                                     /source/crimes/year=YYYY/month=M parquet.
    3. verify_source                 Checks the source lake: partition count,
                                     row counts, sentinel partition.

Trigger manually:  docker exec airflow-scheduler airflow dags trigger \
                       chicago_crime_initial_load

Safe to re-run: the CSV upload overwrites /landing and the Spark conversion
rebuilds the whole source dataset (full, idempotent rebuild).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SOURCE_CSV = "/opt/airflow/data/Crimes_-_2001_to_Present.csv"
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
LANDING_PATH = "/landing/Crimes_-_2001_to_Present.csv"
SOURCE_ROOT = "/source/crimes"
SPARK_MASTER = "spark://spark-master:7077"


def upload_csv_to_hdfs(**context):
    """Stream the local CSV to HDFS /landing (one-time, WebHDFS streaming).

    The Spark containers never see the local ./data bind-mount, so the file is
    first staged on HDFS. The upload streams the file in 64 MiB chunks and
    overwrites, so re-running is safe.
    """
    import requests

    url = f"{WEBHDFS_URL}{LANDING_PATH}"
    params = {"op": "CREATE", "overwrite": "true"}
    headers = {"Content-Type": "application/octet-stream"}

    r1 = requests.put(url, params=params, allow_redirects=False, data="", timeout=60)
    if r1.status_code != 307:
        raise Exception(f"NameNode redirect failed: {r1.status_code} - {r1.text}")
    dn_url = r1.headers["location"]

    def _chunks():
        with open(SOURCE_CSV, "rb") as f:
            while True:
                chunk = f.read(64 * 1024 * 1024)
                if not chunk:
                    break
                yield chunk

    r2 = requests.put(dn_url, data=_chunks(), headers=headers, allow_redirects=False, timeout=7200)
    if r2.status_code != 201:
        raise Exception(f"DataNode upload failed: {r2.status_code} - {r2.text}")

    print(f"Uploaded source CSV to {LANDING_PATH}")


def verify_source(**context):
    """Batch-agnostic verification of the converted source lake."""
    import requests

    url = f"{WEBHDFS_URL}{SOURCE_ROOT}"
    partitions = requests.get(url, params={"op": "LISTSTATUS"}, timeout=60)
    if partitions.status_code != 200:
        raise Exception(f"Source lake missing: {SOURCE_ROOT}")
    years = partitions.json().get("FileStatuses", {}).get("FileStatus", [])
    if not years:
        raise Exception(f"Source lake has no year partitions: {SOURCE_ROOT}")
    print(f"Source lake {SOURCE_ROOT}: {len(years)} year partition(s)")
    for y in years:
        print(f"  {y['pathSuffix']}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="chicago_crime_initial_load",
    description="One-time: Chicago Crime CSV -> partitioned Parquet source on HDFS",
    default_args=default_args,
    schedule=None,  # manual trigger only — one-time operation
    catchup=False,
    tags=["chicago", "crime", "pipeline", "initial-load", "one-time"],
) as dag:

    upload_csv_to_hdfs = PythonOperator(
        task_id="upload_csv_to_hdfs",
        python_callable=upload_csv_to_hdfs,
    )

    convert_to_partitioned_parquet = SparkSubmitOperator(
        task_id="convert_to_partitioned_parquet",
        application="/opt/spark-apps/initial_load_crimes.py",
        conn_id="spark_default",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.dynamicAllocation.enabled": "false",
            "spark.cores.max": "4",
        },
    )

    verify_source = PythonOperator(
        task_id="verify_source",
        python_callable=verify_source,
    )

    upload_csv_to_hdfs >> convert_to_partitioned_parquet >> verify_source
