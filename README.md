# Chicago Crime Data Engineering Pipeline

A fully local, Dockerized **batch data engineering pipeline** that processes the Chicago Crime Dataset into analytics-ready Parquet on HDFS and exposes it through Spark SQL for Power BI.

```text
Chicago Crime CSV
       │
       ▼
    Airflow
       │
       ▼
 HDFS /raw/YYYY-MM
       │
       ▼
 Spark 3.5.1
   ┌───┴────┐
   ▼        ▼
/curated  /rejected
   │
   ▼
Spark Thrift Server :10000
   │
   ▼
 Power BI
```

Everything runs locally through Docker Compose. The pipeline is batch-oriented, so Kafka is intentionally not included.

## Architecture

| Layer | Technology | Responsibility |
|---|---|---|
| Source | Chicago Crime CSV | Raw input |
| Orchestration | Airflow | Monthly batch workflow |
| Storage | HDFS | Raw, curated and rejected data |
| Processing | Apache Spark 3.5.1 | Cleaning, DQ and Parquet |
| Serving | Spark Thrift Server | SQL access on port `10000` |
| Infrastructure | Docker Compose | Local reproducible environment |

## Requirements

- Docker + Docker Compose
- At least **16 GB RAM** recommended
- ~2 GB free disk space for the source dataset, plus Docker/HDFS storage
- The following files are **not included in Git** and must be downloaded manually:

### 1. Chicago Crime Dataset

Download:

[Chicago Crime Dataset — Kaggle](https://www.kaggle.com/datasets/nathaniellybrand/chicago-crime-dataset-2001-present/data?utm_source=chatgpt.com)

The required file is:

```text
Crimes_-_2001_to_Present.csv
```

Place it at:

```text
data/Crimes_-_2001_to_Present.csv
```

The dataset is approximately **1.85 GB**.

### 2. Apache Spark 3.5.1

Download the required Spark distribution:

[Apache Spark 3.5.1 Release](https://spark.apache.org/releases/spark-release-3-5-1.html?utm_source=chatgpt.com)

Required file:

```text
spark-3.5.1-bin-hadoop3.tgz
```

Place it at:

```text
airflow/docker/spark-3.5.1-bin-hadoop3.tgz
```

This file is intentionally **gitignored** because it is ~382 MB.

## Quick Start

Clone the repository and switch to the pipeline branch:

```bash
git clone <repository-url>
cd Chicago-Crime-Pipeline
git switch refactor/chicago-crime-pipeline
```

Download the two required files described above, then verify:

```bash
ls -lh data/Crimes_-_2001_to_Present.csv
ls -lh airflow/docker/spark-3.5.1-bin-hadoop3.tgz
```

Start the stack:

```bash
docker compose up -d --build
```

Check the services:

```bash
docker compose ps
```

## Service URLs

| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| HDFS DataNode | http://localhost:9864 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Airflow | http://localhost:8082 |
| Spark Thrift Server | `jdbc:hive2://localhost:10000` |

Airflow default credentials are `admin` / `admin`.

## Run the Pipeline

The DAG is intentionally manual. **One DAG run processes one month.**

Open:

```text
http://localhost:8082
```

Trigger:

```text
chicago_crime_pipeline
```

and provide:

```json
{"batch_month": "2001-01"}
```

Or from the terminal:

```bash
docker exec airflow-scheduler airflow dags trigger \
  --conf '{"batch_month":"2001-01"}' \
  chicago_crime_pipeline
```

The workflow is:

```text
ingest_raw_to_hdfs
        ↓
spark_dq_and_curate
        ↓
verify_curated_parquet
```

## Data Flow

For a batch such as `2001-01`:

```text
CSV
 ↓
HDFS /raw/2001-01/crime.csv
 ↓
Spark
 ├── /curated/year=2001/month=1/
 └── /rejected/2001-01/
```

The pipeline streams the source CSV and only sends records belonging to the requested month to HDFS.

Spark applies data-quality checks including:

- Null IDs
- Missing crime types
- Invalid dates
- Year/date mismatches
- Invalid coordinates
- Duplicate IDs

Every run produces a reconciliation:

```text
raw = curated + rejected
```

Rerunning the same month is idempotent.

## Spark SQL / Power BI

Curated Parquet is exposed through the external Hive table:

```text
default.curated_crimes
```

Test it with:

```bash
docker exec spark-thrift-server beeline \
  -u jdbc:hive2://localhost:10000 \
  -e 'SELECT count(*) FROM default.curated_crimes;'
```

Power BI connects through the Spark Thrift Server on:

```text
localhost:10000
```

## Reset

To completely reset the local environment:

```bash
docker compose down -v
docker compose up -d --build
```

Without `-v`, Docker volumes are preserved.

## Important

The following large files are **never committed to Git**:

```text
data/
airflow/docker/spark-3.5.1-bin-hadoop3.tgz
```

Anyone cloning the repository must download them manually before building the project.