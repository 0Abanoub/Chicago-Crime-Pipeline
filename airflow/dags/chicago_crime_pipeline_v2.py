"""
Chicago Crime Pipeline v2 — partitioned-source monthly orchestration
=====================================================================
One DAG run == ONE monthly batch. The target month comes from the
`batch_month` DAG parameter (format YYYY-MM, e.g. "2001-05").

The source CSV is converted ONCE by chicago_crime_initial_load into
/source/crimes partitioned by year/month. This DAG NEVER reads the 1.85 GB CSV:
the Spark task reads exactly one partition

    /source/crimes/year=YYYY/month=M

    validate_batch_month            Validates batch_month and asserts the source
                                    partition exists on HDFS (fail-fast).
            |
            v
    process_month                   SparkSubmitOperator -> process_crimes_partitioned.py
                                    reads ONLY source/year=YYYY/month=M, applies DQ,
                                    writes /curated (valid) + /rejected/YYYY-MM (invalid)
            |
            v
    ensure_sql_table                beeline -> CREATE TABLE IF NOT EXISTS +
                                    MSCK REPAIR TABLE default.curated_crimes
                                    (registers the new partition in the Thrift
                                    Server's metastore; also self-heals if the
                                    Thrift Server restarted)
            |
            v
    verify_month                    WebHDFS checks on curated/rejected outputs +
                                    JDBC (beeline) check that the month is now
                                    queryable through the Thrift Server

Idempotency: the Spark task overwrites only its own year/month curated
partition (dynamic partition overwrite) and its own /rejected/YYYY-MM batch;
ensure_sql_table is idempotent; so re-running a month deterministically
replaces that month's outputs without touching any other month.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
SPARK_MASTER = "spark://spark-master:7077"
SOURCE_ROOT = "/source/crimes"
CURATED_ROOT = "/curated"
REJECTED_ROOT = "/rejected"
TABLE_NAME = "default.curated_crimes"
THRIFT_JDBC = "jdbc:hive2://spark-thrift-server:10000"
REGISTER_SQL = "/opt/spark-apps/register_curated_crimes.sql"


def _parse_batch_month(batch_month: str) -> tuple[int, int]:
    """'2001-05' -> (2001, 5). Raises a clear error on bad values."""
    if not isinstance(batch_month, str):
        raise ValueError(f"batch_month must be a YYYY-MM string, got: {batch_month!r}")
    parts = batch_month.split("-")
    if len(parts) != 2:
        raise ValueError(f"batch_month must be YYYY-MM, got: {batch_month!r}")
    try:
        year, month = int(parts[0]), int(parts[1])
    except ValueError:
        raise ValueError(f"batch_month must be YYYY-MM, got: {batch_month!r}") from None
    if not (2001 <= year <= 2100):
        raise ValueError(f"batch_month year out of range (2001-2100): {batch_month!r}")
    if not (1 <= month <= 12):
        raise ValueError(f"batch_month month out of range (1-12): {batch_month!r}")
    return year, month


def _mk_dirs_if_missing(paths):
    """Best-effort creation of HDFS data-lake zones (idempotent)."""
    import requests

    for path in paths:
        url = f"{WEBHDFS_URL}{path}"
        requests.put(url, params={"op": "MKDIRS"}, timeout=60)


def _webhdfs_list_status(path: str) -> list:
    import requests

    resp = requests.get(f"{WEBHDFS_URL}{path}", params={"op": "LISTSTATUS"}, timeout=60)
    if resp.status_code != 200:
        raise Exception(f"WebHDFS LISTSTATUS failed for {path}: {resp.status_code} - {resp.text}")
    return resp.json().get("FileStatuses", {}).get("FileStatus", [])


def validate_batch_month(**context):
    """Validate the parameter and assert the source partition is ready."""
    import requests

    batch_month = context["params"]["batch_month"]
    batch_year, batch_month_num = _parse_batch_month(batch_month)
    partition = f"{SOURCE_ROOT}/year={batch_year}/month={batch_month_num}"

    _mk_dirs_if_missing([SOURCE_ROOT, CURATED_ROOT, REJECTED_ROOT])

    resp = requests.get(f"{WEBHDFS_URL}{partition}", params={"op": "LISTSTATUS"}, timeout=60)
    if resp.status_code != 200:
        raise Exception(
            f"Source partition missing: {partition} — has the initial load run? "
            f"(trigger chicago_crime_initial_load once)"
        )
    files = resp.json().get("FileStatuses", {}).get("FileStatus", [])
    if not files:
        raise Exception(f"Source partition empty: {partition}")
    print(f"Source partition ready: {partition} ({len(files)} file(s))")


def verify_month(**context):
    """Verify THIS month's curated/rejected outputs on HDFS and SQL visibility."""
    import subprocess

    import requests

    batch_month = context["params"]["batch_month"]
    batch_year, batch_month_num = _parse_batch_month(batch_month)
    curated_partition = f"{CURATED_ROOT}/year={batch_year}/month={batch_month_num}"
    rejected_batch = f"{REJECTED_ROOT}/{batch_month}"

    # 1. HDFS outputs
    curated_files = _webhdfs_list_status(curated_partition)
    if not curated_files:
        raise Exception(f"Curated partition missing or empty: {curated_partition}")
    rejected_files = _webhdfs_list_status(rejected_batch)
    if not rejected_files:
        raise Exception(f"Rejected output missing or empty: {rejected_batch}")

    # 2. SQL visibility through the Thrift Server (beeline, JDBC)
    expected_partition = f"year={batch_year}/month={batch_month_num}"
    sql = (
        f"SHOW PARTITIONS {TABLE_NAME};\n"
        f"SELECT year, month, COUNT(*) AS cnt FROM {TABLE_NAME} "
        f"GROUP BY year, month ORDER BY year, month;\n"
        f"SELECT COUNT(*) AS total FROM {TABLE_NAME};"
    )
    proc = subprocess.run(
        ["beeline", "-u", THRIFT_JDBC, "--silent=true", "-e", sql],
        capture_output=True, text=True, timeout=600,
    )
    if proc.returncode != 0:
        raise Exception(f"beeline verification failed:\n{proc.stdout}\n{proc.stderr}")
    print(proc.stdout)

    # Exact token match against the SHOW PARTITIONS output (a substring test
    # would falsely pass year=2001/month=1 when only year=2001/month=10 exists).
    # beeline --silent wraps values in '|  value  |', so strip pipes first.
    visible_partitions = set()
    for line in proc.stdout.splitlines():
        token = line.strip().strip("|").strip()
        if token.startswith("year=") and "/month=" in token:
            visible_partitions.add(token)
    if expected_partition not in visible_partitions:
        raise Exception(
            f"SQL layer does NOT show partition {expected_partition} "
            f"for batch {batch_month} — table registration/repair failed"
        )
    print(f"Batch {batch_month} verified:")
    print(f"  curated  {curated_partition}  ({len(curated_files)} parquet file(s))")
    print(f"  rejected {rejected_batch}      ({len(rejected_files)} parquet file(s))")
    print(f"  SQL      partition {expected_partition} visible via {THRIFT_JDBC}")


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
    dag_id="chicago_crime_pipeline_v2",
    description="Chicago Crime: read one /source partition -> Spark DQ -> Curated/Rejected Parquet -> Thrift SQL",
    default_args=default_args,
    schedule=None,  # manual trigger with {"batch_month": "YYYY-MM"}
    catchup=False,
    params={"batch_month": "2001-01"},  # sensible default; override via trigger conf
    tags=["chicago", "crime", "pipeline", "batch", "v2"],
) as dag:

    validate_batch_month = PythonOperator(
        task_id="validate_batch_month",
        python_callable=validate_batch_month,
    )

    process_month = SparkSubmitOperator(
        task_id="process_month",
        application="/opt/spark-apps/process_crimes_partitioned.py",
        conn_id="spark_default",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
            "spark.dynamicAllocation.enabled": "false",
            "spark.cores.max": "4",
        },
        application_args=["{{ params.batch_month }}"],
    )

    ensure_sql_table = BashOperator(
        task_id="ensure_sql_table",
        bash_command=f"beeline -u {THRIFT_JDBC} --silent=true -f {REGISTER_SQL}",
    )

    verify_month = PythonOperator(
        task_id="verify_month",
        python_callable=verify_month,
    )

    validate_batch_month >> process_month >> ensure_sql_table >> verify_month
