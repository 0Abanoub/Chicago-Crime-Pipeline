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
