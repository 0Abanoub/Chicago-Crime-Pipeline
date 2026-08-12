"""
Chicago Crime Pipeline — Airflow orchestration
================================================
End-to-end batch pipeline that turns one monthly slice of the immutable
Chicago Crime CSV into analytics-ready Parquet on HDFS:

    ingest_raw_to_hdfs -> spark_dq_and_curate -> verify_curated_parquet

ONE DAG run == ONE monthly batch. The target month is taken from the
`batch_month` DAG parameter (format YYYY-MM, e.g. "2001-01"). Every task in
the run operates on that same month.

    1. ingest_raw_to_hdfs    Streams the source CSV line-by-line (never
                             loading it into memory) and writes ONLY the
                             records whose crime date falls inside
                             [YYYY-MM-01, YYYY-MM+1-01) to
                             /raw/<batch_month>/crime.csv
    2. spark_dq_and_curate   SparkSubmitOperator -> process_crimes.py runs on
                             the standalone cluster, applies row-level data
                             quality checks, writes valid rows to
                             /curated (Parquet, partitioned by year/month)
                             and invalid rows to /rejected/<batch_month>
    3. verify_curated_parquet  Batch-aware check that THIS month's raw batch,
                             curated partition and rejected output exist

The `spark_default` connection stores the master URL literally
(host='spark://spark-master:7077', port=None) because the Spark provider
builds --master as <host>:<port> and cannot handle a bare host:port pair.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SOURCE_CSV = "/opt/airflow/data/Crimes_-_2001_to_Present.csv"
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
SPARK_MASTER = "spark://spark-master:7077"
DATE_FMT = "%m/%d/%Y %I:%M:%S %p"


def _parse_batch_month(batch_month: str) -> tuple[int, int]:
    """'2001-05' -> (2001, 5). Raises a clear error on bad values."""
    parts = batch_month.split("-")
    if len(parts) != 2:
        raise ValueError(f"batch_month must be YYYY-MM, got: {batch_month!r}")
    year, month = int(parts[0]), int(parts[1])
    if not (1 <= month <= 12):
        raise ValueError(f"batch_month month out of range: {batch_month!r}")
    return year, month


def _mk_dirs_if_missing(client_paths):
    """Best-effort creation of the standard HDFS data-lake zones (idempotent)."""
    import requests

    for path in client_paths:
        url = f"{WEBHDFS_URL}{path}"
        requests.put(url, params={"op": "MKDIRS"}, timeout=60)


def ingest_raw_to_hdfs(**context):
    """Stream-filter the source CSV for one month and write it to HDFS.

    Why streaming: the source CSV is ~1.85 GB / ~7.8 M rows, so we must never
    load it (or a Python object per row) into memory. Rows are read one line
    at a time, the crime date is parsed, and only rows that belong to the
    target month are forwarded. The filtered result is pushed straight into a
    WebHDFS PUT (generator body), so no temporary copy is created either.

    Rows whose date cannot be parsed are counted and forwarded to the batch as
    well, so nothing is silently dropped — the Spark DQ step later flags them
    as INVALID_DATE and routes them to /rejected. This keeps
    raw = curated + rejected true for the batch.
    """
    import csv

    import requests
    from datetime import datetime as _dt

    batch_month = context["params"]["batch_month"]
    batch_year, batch_month_num = _parse_batch_month(batch_month)
    hdfs_path = f"/raw/{batch_month}/crime.csv"

    _mk_dirs_if_missing(["/raw", "/curated", "/rejected"])

    nn_url = f"{WEBHDFS_URL}{hdfs_path}"
    params = {"op": "CREATE", "overwrite": "true"}
    headers = {"Content-Type": "application/octet-stream"}

    # Step 1: ask the NameNode where to write (expects a 307 redirect)
    r1 = requests.put(nn_url, params=params, allow_redirects=False, data="", timeout=60)
    if r1.status_code != 307:
        raise Exception(f"NameNode redirect failed: {r1.status_code} - {r1.text}")
    dn_url = r1.headers["location"]

    def _month_rows():
        """Generator: yield the header + the rows that belong to the batch."""
        scanned = selected = malformed = 0
        with open(SOURCE_CSV, newline="", encoding="utf-8", errors="replace") as f:
            header = f.readline()
            date_idx = next(csv.reader([header])).index("Date")
            yield header
            for line in f:
                scanned += 1
                try:
                    row = next(csv.reader([line]))
                    dt = _dt.strptime(row[date_idx], DATE_FMT)
                except (ValueError, IndexError):
                    malformed += 1
                    yield line  # surface via DQ -> /rejected, don't drop silently
                    continue
                if dt.year == batch_year and dt.month == batch_month_num:
                    selected += 1
                    yield line
        print(f"Batch: {batch_month}")
        print(f"Rows scanned: {scanned}")
        print(f"Rows selected: {selected}")
        print(f"Rows with unparseable date (written to batch, DQ will reject): {malformed}")
        print(f"Rows written: {selected + malformed}")
        print(f"Target: {hdfs_path}")

    # Step 2: stream the filtered rows straight to the DataNode
    r2 = requests.put(dn_url, data=_month_rows(), headers=headers, allow_redirects=False, timeout=3600)
    if r2.status_code != 201:
        raise Exception(f"DataNode upload failed: {r2.status_code} - {r2.text}")

    print(f"Successfully wrote {batch_month} batch to {hdfs_path}")


def verify_curated_parquet(**context):
    """Batch-aware check that THIS month made it through raw -> curated.

    Checks the specific batch paths (not just 'does /curated exist'), so a
    broken new batch cannot pass because an older batch already produced data.
    """
    import requests

    batch_month = context["params"]["batch_month"]
    batch_year, batch_month_num = _parse_batch_month(batch_month)

    raw = requests.get(
        f"{WEBHDFS_URL}/raw/{batch_month}/crime.csv", params={"op": "GETFILESTATUS"}, timeout=60
    )
    if raw.status_code != 200:
        raise Exception(f"Batch raw missing: /raw/{batch_month}/crime.csv")
    raw_bytes = raw.json()["FileStatus"]["length"]
    if raw_bytes <= 0:
        raise Exception(f"Batch raw is empty (0 bytes): /raw/{batch_month}/crime.csv")

    curated_dir = f"/curated/year={batch_year}/month={batch_month_num}"
    curated = requests.get(f"{WEBHDFS_URL}{curated_dir}", params={"op": "LISTSTATUS"}, timeout=60)
    if curated.status_code != 200:
        raise Exception(f"Curated partition missing: {curated_dir}")
    curated_files = curated.json().get("FileStatuses", {}).get("FileStatus", [])
    if not curated_files:
        raise Exception(f"Curated partition empty: {curated_dir}")

    rejected = requests.get(
        f"{WEBHDFS_URL}/rejected/{batch_month}", params={"op": "LISTSTATUS"}, timeout=60
    )
    if rejected.status_code != 200:
        raise Exception(f"Rejected output missing: /rejected/{batch_month}")
    rejected_files = rejected.json().get("FileStatuses", {}).get("FileStatus", [])
    if not rejected_files:
        raise Exception(f"Rejected output empty: /rejected/{batch_month}")

    print(f"Batch {batch_month} verified:")
    print(f"  raw      /raw/{batch_month}/crime.csv          ({raw_bytes} bytes)")
    print(f"  curated  {curated_dir}                         ({len(curated_files)} parquet files)")
    print(f"  rejected /rejected/{batch_month}               ({len(rejected_files)} parquet files)")


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
    dag_id="chicago_crime_pipeline",
    description="Chicago Crime CSV -> HDFS Raw (one month per run) -> Spark DQ -> Curated Parquet -> Thrift SQL",
    default_args=default_args,
    schedule=None,  # manual trigger with {"batch_month": "YYYY-MM"}, or cron per month
    catchup=False,
    params={"batch_month": "2001-01"},  # sensible default; override via trigger conf
    tags=["chicago", "crime", "pipeline", "batch"],
) as dag:

    ingest_raw_to_hdfs = PythonOperator(
        task_id="ingest_raw_to_hdfs",
        python_callable=ingest_raw_to_hdfs,
    )

    spark_dq_and_curate = SparkSubmitOperator(
        task_id="spark_dq_and_curate",
        application="/opt/spark-apps/process_crimes.py",
        conn_id="spark_default",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.dynamicAllocation.enabled": "false",
            "spark.cores.max": "4",
        },
        application_args=["{{ params.batch_month }}"],
    )

    verify_curated_parquet = PythonOperator(
        task_id="verify_curated_parquet",
        python_callable=verify_curated_parquet,
    )

    ingest_raw_to_hdfs >> spark_dq_and_curate >> verify_curated_parquet
