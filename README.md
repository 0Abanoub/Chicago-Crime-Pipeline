<<<<<<< HEAD
# Chicago Crime Data Engineering Pipeline

## 1. Project Overview

This project implements a **batch-oriented data engineering pipeline** for processing the Chicago Crime Dataset.

The pipeline is designed to handle large historical crime datasets, perform distributed data processing and data quality validation, store the processed data in a columnar format, and expose the data through **Spark Thrift Server** so that **Power BI** can query it for analytics and visualization.

The architecture is fully local and containerized using Docker, with no cloud services required.

---

## 2. Overall Architecture

```text
Chicago Crime CSV
       │
       ▼
    Airflow
       │
       ▼
      HDFS
   Raw Data Zone
       │
       ▼
     Spark
 Processing + Data Quality
       │
       ▼
      HDFS
 Curated Parquet
       │
       ▼
Spark Thrift Server
       │
     JDBC/ODBC
       │
       ▼
    Power BI
```

The architecture separates the main responsibilities into four layers:

1. **Orchestration** — Airflow
2. **Storage** — HDFS
3. **Processing** — Apache Spark
4. **Serving & Visualization** — Spark Thrift Server and Power BI

---

## 3. Data Source

The project uses the **Chicago Crime Dataset**, containing historical crime records from approximately 2001 to 2023.

The original dataset is approximately:

- **1.85 GB**
- **~7.8 million records**
- **22 columns**
- CSV format

The source file is treated as the original, immutable input. The pipeline does not modify the source data directly.

---

## 4. Workflow Orchestration — Apache Airflow

**Apache Airflow** is responsible for orchestrating the complete batch workflow.

Airflow does not perform the actual data processing. Instead, it controls **when each step runs and in which order**.

A typical workflow is:

```text
Detect New Batch
      ↓
Validate Source
      ↓
Store Raw Data
      ↓
Run Spark Processing
      ↓
Run Data Quality Checks
      ↓
Write Curated Parquet
      ↓
Make Data Available Through Spark
      ↓
Complete Batch
```

Airflow also provides:

- Scheduling
- Task dependencies
- Retries
- Failure handling
- Execution monitoring
- Batch tracking

This allows new batches to be processed automatically without manually running every step.

---

## 5. Data Lake — HDFS

**HDFS** is used as the project's distributed storage layer.

The data lake is organized into logical zones:

```text
HDFS
│
├── raw/
│
├── processed/
│
├── curated/
│
└── rejected/
```

### Raw Zone

The raw zone stores the original source files exactly as received.

```text
/raw/{batch_id}/crime.csv
```

The raw data is preserved so that the pipeline can always reproduce the processing from the original source.

### Processed Zone

The processed zone can contain intermediate Spark outputs when required during transformation.

### Curated Zone

The curated zone contains validated and transformed data in **Parquet** format.

The data can be partitioned by year:

```text
/curated/
    year=2001/
    year=2002/
    ...
    year=2023/
```

### Rejected Zone

Records that fail row-level data quality checks are stored separately with a reason explaining why they were rejected.

---

## 6. Distributed Processing — Apache Spark

**Apache Spark** is responsible for the computational part of the pipeline.

Spark reads the raw data from HDFS and performs:

- Schema enforcement
- Data type conversion
- Timestamp parsing
- Data cleaning
- Null handling
- Duplicate detection
- Data validation
- Feature engineering
- Aggregations
- Transformation
- Writing curated Parquet

The important distinction is:

> **HDFS stores the data; Spark processes the data.**

Spark does not replace HDFS. It uses HDFS as its storage source and destination.

Conceptually:

```text
HDFS
  │
  │ Read
  ▼
Spark
  │
  │ Process
  ▼
HDFS
  │
  │ Write
  ▼
Curated Parquet
```

---

## 7. Data Quality

Data quality validation is implemented inside the Spark processing workflow.

Examples include:

| Check | Purpose |
|---|---|
| ID not null | Ensure every crime has an identifier |
| ID uniqueness | Detect duplicate records |
| Date validity | Ensure dates can be parsed |
| Primary Type validity | Ensure crime type exists |
| Boolean validation | Validate Arrest and Domestic |
| Geographic validation | Validate latitude and longitude |
| Year consistency | Compare Date year with Year field |
| Row-count reconciliation | Ensure no records disappear silently |

Invalid individual records are moved to the **rejected zone**.

Critical batch-level failures stop the pipeline so that invalid data does not continue downstream.

---

## 8. Curated Data — Parquet

After processing and validation, Spark writes the trusted dataset to HDFS in **Parquet** format.

Parquet is used instead of CSV because it provides:

- Columnar storage
- Compression
- Explicit data types
- Faster analytical reads
- Efficient column selection
- Better compatibility with Spark

For example, a query requiring only:

```text
year
primary_type
latitude
longitude
```

does not need to read every column from the Parquet files.

Parquet therefore becomes the main **analytics-ready format** inside the data lake.

---

## 9. SQL Serving Layer — Spark Thrift Server

The **Spark Thrift Server** provides the SQL access layer between Spark/HDFS and Power BI.

Power BI should not directly manage HDFS files.

Instead:

```text
Power BI
   │
   │ JDBC / ODBC
   ▼
Spark Thrift Server
   │
   ▼
Spark SQL
   │
   ▼
Parquet on HDFS
```

The Thrift Server exposes Spark through a SQL interface.

When Power BI requests data, Spark can query the curated Parquet files in HDFS and return the query result.

This means that a separate PostgreSQL warehouse is not required.

The architecture therefore avoids maintaining another full copy of the curated dataset.

---

## 10. Power BI

**Power BI** is the final visualization and analytics layer.

Power BI connects to the **Spark Thrift Server** using an appropriate JDBC/ODBC connection.

The dashboard can provide:

- Total crime count
- Crime trends by year
- Crime trends by month
- Crime type analysis
- Arrest statistics
- Domestic crime analysis
- Crime by police district
- Crime by community area
- Geographic analysis
- Maps and heatmaps

The Power BI model remains connected to the same SQL endpoint while the underlying Parquet data changes.

When a new batch is processed, the same queries can return the updated data after refresh.

---

## 11. Batch Processing Lifecycle

When a new crime-data batch arrives, the process is:

```text
1. New CSV arrives
        ↓
2. Airflow detects the batch
        ↓
3. File is stored in HDFS Raw
        ↓
4. Spark reads the raw CSV
        ↓
5. Spark applies schema and cleaning
        ↓
6. Spark performs Data Quality checks
        ↓
7. Invalid records → HDFS Rejected
        ↓
8. Valid records → HDFS Curated
        ↓
9. Curated data stored as Parquet
        ↓
10. Spark Thrift Server exposes the data
        ↓
11. Power BI queries the data
        ↓
12. Dashboard reflects the updated batch
```

---

## 12. Why There Is No PostgreSQL

PostgreSQL was intentionally removed from the final architecture.

The original design used PostgreSQL as a separate serving layer for Power BI. However, this would require maintaining another copy of the processed data.

For example:

```text
HDFS → Parquet
       ↓
PostgreSQL
       ↓
Power BI
```

This introduces additional storage, loading logic, database management, and another component to maintain.

The final architecture instead uses:

```text
HDFS → Parquet
          ↓
       Spark
          ↓
   Thrift Server
          ↓
      Power BI
```

This allows the project to keep the main dataset in HDFS while using Spark as both the processing engine and query engine.

---

## 13. Why Kafka Is Not Used

Kafka is intentionally excluded.

The project processes **historical and batch-arriving data**, not a continuous real-time event stream.

Kafka would introduce a streaming infrastructure that is not required for the current business problem.

If the project later received continuous crime events in real time, Kafka could be introduced as an ingestion layer.

---

## 14. Docker Infrastructure

The complete environment is containerized using **Docker and Docker Compose**.

The infrastructure contains the required services for:

- Airflow
- HDFS
- Spark
- Spark Thrift Server
- Supporting databases/services required by Airflow

Containerization provides:

- Reproducibility
- Easier setup
- Consistent environments
- Easier project demonstration
- Isolation between services

The project does not require cloud infrastructure or paid services.

---

## 15. Final Technology Responsibilities

| Technology | Responsibility |
|---|---|
| **Chicago Crime CSV** | Source data |
| **Airflow** | Workflow orchestration and automation |
| **HDFS** | Data lake storage |
| **Spark / PySpark** | Distributed processing and transformation |
| **Spark SQL** | SQL-based processing and querying |
| **Parquet** | Curated analytical storage format |
| **Spark Thrift Server** | SQL serving endpoint |
| **JDBC / ODBC** | Connection between Power BI and Spark |
| **Power BI** | Visualization and analytics |
| **Docker** | Containerized infrastructure |
| **Git / GitHub** | Version control |

---

## 16. Final Architecture

The final architecture can be summarized as:

```text
                 ┌─────────────┐
                 │   Airflow   │
                 │ Orchestration│
                 └──────┬──────┘
                        │
                        ▼
              ┌──────────────────┐
              │       HDFS       │
              │    Raw Data      │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │      Spark       │
              │ Clean / Transform│
              │   Data Quality   │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │       HDFS       │
              │ Curated Parquet  │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Spark Thrift     │
              │     Server       │
              └────────┬─────────┘
                       │
                    JDBC/ODBC
                       │
                       ▼
              ┌──────────────────┐
              │     Power BI     │
              │    Dashboard     │
              └──────────────────┘
```

### Core Design Principle

The architecture follows a simple separation of responsibilities:

> **Airflow orchestrates → HDFS stores → Spark processes → Parquet preserves the curated data → Spark Thrift Server exposes SQL → Power BI visualizes.**

The design intentionally avoids unnecessary components such as PostgreSQL, Kafka, and cloud services, while keeping the architecture suitable for large-scale batch processing and future expansion.
=======
# Mini Big Data Lab — HDFS · Spark · Kafka · Kafka UI · Airflow

A single-node teaching stack sized for a 16GB laptop. Everything runs in one
Docker network (`bigdata-net`) so students can point tools at each other by
hostname (`namenode`, `spark-master`, `kafka`, etc).

> **Image note:** Spark uses `bde2020/spark-master` / `bde2020/spark-worker`
> (Spark 3.3.0) and Kafka uses the official `apache/kafka` image (KRaft mode).
> Bitnami's free `bitnami/spark` and `bitnami/kafka` tags were discontinued in
> August 2025 (moved behind a paid "Bitnami Secure Images" subscription), so
> this stack avoids them entirely. The Jupyter and Airflow images install
> `pyspark==3.3.0` to match the cluster's Spark version exactly — if you ever
> bump `SPARK_IMAGE_TAG` in `.env`, update `PYSPARK_VERSION` and
> `SPARK_KAFKA_PACKAGE` to match, or Spark clients will fail to connect.

## Architecture

```
                        ┌─────────────────────┐
                        │      Airflow         │
                        │ (webserver+scheduler) │  :8082
                        └──────────┬───────────┘
                                   │ orchestrates
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
   ┌────▼────┐              ┌──────▼──────┐            ┌──────▼──────┐
   │  HDFS   │◄────────────►│    Spark    │◄──────────►│    Kafka    │
   │ NN+DN   │   read/write │ master+wkr  │  streaming │  (KRaft)    │
   │ :9870   │              │   :8080     │            │   :9092     │
   └─────────┘              └──────┬──────┘            └──────┬──────┘
                                   │                          │
                             ┌─────▼─────┐              ┌─────▼─────┐
                             │  Jupyter  │              │ Kafka UI  │
                             │   :8888   │              │   :8090   │
                             └───────────┘              └───────────┘
```

## Quick start

```bash
cd bigdata-lab
docker compose up -d --build
```

First boot takes a few minutes (image pulls + Airflow DB migration). Check status:

```bash
docker compose ps
```

## Service URLs (default ports, edit in `.env` if they collide)

| Service           | URL                          | Notes                          |
|-------------------|------------------------------|---------------------------------|
| HDFS Namenode UI  | http://localhost:9870        | browse filesystem, live nodes  |
| HDFS Datanode UI  | http://localhost:9864        |                                  |
| Spark Master UI   | http://localhost:8080        | see running/completed jobs     |
| Spark Worker UI   | http://localhost:8081        |                                  |
| Jupyter Lab       | http://localhost:8888        | token: `lab`                   |
| Kafka UI          | http://localhost:8090        | topics, messages, consumer groups |
| Kafka (in-network)| kafka:9092                   | for containers on bigdata-net  |
| Kafka (from host) | localhost:9094               | for CLI tools on your laptop   |
| Airflow           | http://localhost:8082        | user/pass in `.env` (admin/admin) |

## Resource footprint (16GB profile)

This is tuned to comfortably fit 16GB total, leaving headroom for your OS:
- Spark worker: 3GB / 2 cores (adjust `SPARK_WORKER_MEMORY` / `SPARK_WORKER_CORES` in compose)
- Single HDFS datanode, single Kafka broker (combined controller+broker, KRaft, no Zookeeper)
- Airflow LocalExecutor (no Redis/Celery worker containers)

If things feel tight, stop services you're not actively teaching with, e.g.:
```bash
docker compose stop airflow-webserver airflow-scheduler
```

## Lab structure

### 1. HDFS labs
```bash
docker exec -it namenode hdfs dfs -mkdir -p /labs/demo
docker exec -it namenode hdfs dfs -put /etc/hosts /labs/demo/
docker exec -it namenode hdfs dfs -ls /labs/demo
docker exec -it namenode hdfs dfs -cat /labs/demo/hosts
```
Good topics: block replication (set to 1 here since we have 1 datanode — explain
why production clusters use 3), permissions, `hdfs fsck`, the NameNode UI.

### 2. Kafka labs
The official `apache/kafka` image keeps its CLI scripts under `/opt/kafka/bin/`:
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic demo \
  --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --topic demo --bootstrap-server kafka:9092
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic demo --bootstrap-server kafka:9092 --from-beginning
```
Use Kafka UI (http://localhost:8090) to visually show partitions, offsets, and
consumer group lag — much easier for students than raw CLI output.

### 3. Spark labs
Three ready-made jobs in `spark/apps/` (mounted into the master/worker at
`/opt/spark-apps` and into Jupyter at `~/work/spark-apps`):

- `batch_wordcount.py` — batch processing, HDFS in/out
- `streaming_kafka_to_hdfs.py` — Structured Streaming, Kafka → HDFS
- `spark_sql_demo.py` — Spark SQL over a CSV in HDFS

Each file has run instructions in its docstring. Submit with:
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  /opt/spark-apps/batch_wordcount.py
```
Or run interactively from Jupyter — see `00_environment_check.ipynb` for the
connection boilerplate (note: connect to `spark://spark-master:7077`, not
local mode, so students see it as a real cluster).

### 4. Integration / pipeline labs
`airflow/dags/pipeline_hdfs_kafka_spark.py` is a 3-task DAG:
`ensure_hdfs_dirs → produce_kafka_events → spark_batch_job`, all wired through
Airflow. Trigger it from the Airflow UI (http://localhost:8082) and watch task
logs to see the whole path lit up in one place. Good scaffold for students to
extend (e.g. add a streaming sensor task, add a Slack/email notify task, swap
in `SparkSubmitOperator` once they're comfortable with the basics).

## Use-case coverage checklist
- **Batch**: `batch_wordcount.py`, HDFS CLI labs
- **Streaming**: `streaming_kafka_to_hdfs.py`, Kafka producer/consumer labs
- **Spark SQL**: `spark_sql_demo.py`
- **Pipeline/orchestration**: Airflow DAG tying all three systems together

## Resetting between class sessions
```bash
docker compose down -v   # wipes HDFS/Kafka/Postgres volumes - fresh start
docker compose up -d --build
```
Drop `-v` if you want data to persist across sessions instead.

## Troubleshooting
- **`pip install` fails/times out while building the Airflow image**: `pyspark`
  is a large download (~300MB); on a slow or unstable connection it can get
  interrupted. Just re-run `docker compose up -d --build` — Docker reuses
  already-downloaded layers, and the Dockerfile is set to retry pip 8 times
  with a longer timeout. If it keeps failing, try building just that image
  first so you get a clearer error: `docker compose build airflow-init`.
- **Airflow webserver keeps restarting**: check `docker compose logs airflow-init`
  first — it must finish successfully before webserver/scheduler start.
- **Spark job can't reach HDFS**: confirm you're using `hdfs://namenode:9000/...`
  paths, not `localhost` — containers talk to each other by service name.
- **Kafka client from host laptop**: use `localhost:9094` (the `EXTERNAL`
  listener), not `9092` (in-network only).
- **Port already in use**: change the relevant `*_PORT` value in `.env` and
  re-run `docker compose up -d`.
>>>>>>> origin/main
