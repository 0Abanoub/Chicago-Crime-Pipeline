# Chicago Crime Data Engineering Pipeline

A fully local, Dockerized **batch data engineering pipeline** that converts the
Chicago Crime Dataset (~1.85 GB CSV, 7.8 M rows) into a partitioned Parquet
source on HDFS, runs monthly data-quality batches with Spark, and serves the
curated result to Power BI through a Spark Thrift Server.

```text
                    SOURCE
                       |
                       v
        Crimes_-_2001_to_Present.csv     (one-time)
                       |
                       v
            Spark Initial Load
            (chicago_crime_initial_load)
                       |
                       v
            HDFS /source/crimes
              year=YYYY/month=M
                       |
              MONTHLY AIRFLOW RUN
              batch_month=YYYY-MM
                       |
                       v
        Read ONLY one source partition
                       |
                       v
             Spark Data Quality
             /                  \
          VALID                INVALID
            |                    |
            v                    v
       /curated/            /rejected/
       year=YYYY/           YYYY-MM
       month=M
            |
            v
     Spark Thrift :10000
            |
            v
        Power BI
```

## Architecture

| Layer | Technology | Responsibility |
|---|---|---|
| Source | Chicago Crime CSV | Raw input (converted once) |
| Source lake | HDFS Parquet, `year/month` partitioned | Partitioned source read by monthly runs |
| Orchestration | Airflow | One-time load + monthly batch workflow |
| Processing | Apache Spark 3.5.1 | DQ, cleaning, Parquet writes |
| Serving | Spark Thrift Server | SQL access on port `10000` |
| Infrastructure | Docker Compose | Local reproducible environment |

The pipeline is batch-oriented. **Kafka is intentionally not included** and
PostgreSQL is used only for Airflow's metadata (curated data stays on HDFS).

## Requirements

- Docker + Docker Compose
- At least **16 GB RAM** recommended
- ~2 GB free disk for the source CSV, plus Docker/HDFS storage
- Two large files are **not in Git** and must be placed manually:

### 1. Chicago Crime Dataset

Download [the Kaggle dataset](https://www.kaggle.com/datasets/nathaniellybrand/chicago-crime-dataset-2001-present/data)
and place the file at:

```text
data/Crimes_-_2001_to_Present.csv
```

(~1.85 GB, 7,846,809 data rows, years 2001–2023.)

### 2. Apache Spark 3.5.1

Download [Spark 3.5.1](https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz)
and place it at:

```text
airflow/docker/spark-3.5.1-bin-hadoop3.tgz
```

## Quick Start

```bash
ls -lh data/Crimes_-_2001_to_Present.csv
ls -lh airflow/docker/spark-3.5.1-bin-hadoop3.tgz

docker compose up -d --build
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

Airflow default credentials: `admin` / `admin`.

## HDFS Layout

| Zone | Path | Contents |
|---|---|---|
| Landing | `/landing/Crimes_-_2001_to_Present.csv` | One-time CSV upload |
| Source lake | `/source/crimes/year=YYYY/month=M/` | Partitioned Parquet source |
| Curated | `/curated/year=YYYY/month=M/` | Analytics-ready Parquet (valid rows) |
| Rejected | `/rejected/YYYY-MM/` | Rows failing DQ, per batch |

Month partition values are written unpadded by Spark (e.g. `month=1`,
`month=12`). Year 2001→2023, 12 months each. Rows whose `Date` cannot be
parsed (none in the current file) would be quarantined to the sentinel
partition `/source/crimes/year=0/month=0`; monthly batches never read it.

## Initial Conversion (one-time)

The source CSV is converted **once** by the `chicago_crime_initial_load` DAG:

```text
upload_csv_to_hdfs
        ↓
convert_to_partitioned_parquet
        ↓
verify_source
```

1. `upload_csv_to_hdfs` streams `data/Crimes_-_2001_to_Present.csv` to HDFS
   `/landing` via WebHDFS (the Spark containers never see the local bind
   mount).
2. `convert_to_partitioned_parquet` runs `spark/apps/initial_load_crimes.py`,
   which reads the CSV with the 22-column schema, derives `year`/`month` from
   the parsed `Date` column, and writes `/source/crimes` partitioned by
   year/month.
3. `verify_source` reports the year partitions.

Trigger it once:

```bash
docker exec airflow-scheduler airflow dags trigger chicago_crime_initial_load
```

It is safe to re-run: the upload overwrites `/landing` and the Spark job
rebuilds the whole source lake.

> **Why a source lake?** The old pipeline re-scanned the 1.85 GB CSV on every
> monthly run (~74 s of pure Python file streaming + 7.8 M rows read each
> time). Partitioning once into Parquet (547 MB, 3.4× smaller than the CSV)
> lets a monthly run read only a ~3 MB partition.

> **Schema note:** the source keeps the original `Year` data column under the
> name `YearReported` because Spark treats `Year` and the `year` partition
> column as the same column case-insensitively and would otherwise drop the
> data column. `process_crimes_partitioned.py` restores the name to `Year`
> after reading, so DQ rules and the curated/rejected schemas are unchanged.

## Monthly DAG Execution

The DAG is manual. **One DAG run = one month.**

DAG: `chicago_crime_pipeline_v2`

```text
validate_batch_month
        ↓
process_month
        ↓
ensure_sql_table
        ↓
verify_month
```

Trigger from the Airflow UI (`http://localhost:8082`) or the terminal:

```bash
docker exec airflow-scheduler airflow dags trigger \
  --conf '{"batch_month":"2001-01"}' \
  chicago_crime_pipeline_v2
```

### `batch_month` parameter

Format `YYYY-MM`, e.g. `2001-05`. The DAG validates the format (must be a
string, split on `-`, year 2001–2100, month 1–12) and fails fast with a clear
error. A missing source partition (`/source/crimes/year=YYYY/month=M`) also
fails fast in `validate_batch_month`.

### Task details

1. **validate_batch_month** — validates the parameter and asserts the exact
   source partition exists and is non-empty.
2. **process_month** — SparkSubmitOperator runs
   `spark/apps/process_crimes_partitioned.py <batch_month>`, which reads **only**
   `/source/crimes/year=YYYY/month=M` (partition pruning by construction),
   applies the DQ rules, and writes `/curated` + `/rejected/YYYY-MM`.
3. **ensure_sql_table** — beeline runs `spark/apps/register_curated_crimes.sql`
   (`CREATE TABLE IF NOT EXISTS default.curated_crimes` + `MSCK REPAIR TABLE`),
   registering the new partition in the Thrift Server's metastore and
   self-healing if the Thrift Server restarted.
4. **verify_month** — checks the curated partition and rejected batch on HDFS
   and queries the table through JDBC/beeline to confirm the month is visible
   via SQL.

### Idempotency

Re-running a month deterministically replaces that month's outputs:

- curated: dynamic partition overwrite → only `year=YYYY/month=M` is replaced,
  other months untouched;
- rejected: batch-scoped `/rejected/YYYY-MM` is overwritten;
- `ensure_sql_table`: `CREATE TABLE IF NOT EXISTS` + `MSCK REPAIR` is
  idempotent.

Re-running a month never touches other months and never duplicates data.

## Spark Processing

`process_crimes_partitioned.py` reads the exact partition path, so Spark scans
only that directory:

```text
hdfs://namenode:9000/source/crimes/year=2001/month=5
```

An optional `--explain` flag prints the physical plan for a predicate-filtered
read of the whole source tree, showing the pushed-down
`PartitionFilters: [(year = 2001), (month = 5)]`. The actual read uses the
exact partition path, which is strictly stronger.

## Data Quality Behavior

Identical rules to the original pipeline (`process_crimes.py`), applied row
level:

| Rule | Failing reason |
|---|---|
| `ID_NULL` | `ID` is null |
| `PRIMARY_TYPE_NULL` | `Primary Type` is null |
| `INVALID_DATE` | `Date` cannot be parsed |
| `YEAR_MISMATCH` | parsed `Date` year ≠ `Year` column |
| `NULL_GEO` | `Latitude`/`Longitude` null |
| `INVALID_LAT` | latitude outside Chicago bbox (41.0–43.0) |
| `INVALID_LON` | longitude outside Chicago bbox (-88.5…-87.0) |
| `DUPLICATE_ID` | duplicate `ID` among valid rows (first by `Date` kept) |

Every run prints a reconciliation:

```text
row_count_raw = row_count_curated + row_count_rejected
```

Invalid rows are **not dropped** — they are written to `/rejected/YYYY-MM`
with `reject_reason` listing the failed checks, traceable to the source batch.

## Curated Dataset

`/curated/year=YYYY/month=M` — Parquet, snappy, 21 analytics columns
(original 22 minus `Year`, which collides case-insensitively with the `year`
partition column) plus `year`/`month` partition columns. The schema matches the
original pipeline's curated output, so existing SQL consumers are unchanged.

## Rejected Dataset

`/rejected/YYYY-MM/` — Parquet with all 22 original columns plus
`reject_reason`. Scoped to the source batch for traceability.

## Thrift Server

- Spark Thrift Server on `spark-thrift-server:10000`, published as
  `localhost:10000`.
- Curated Parquet is registered as the external Hive table
  `default.curated_crimes` (`USING parquet PARTITIONED BY (year INT, month INT)
  LOCATION 'hdfs://namenode:9000/curated'`).
- The table is registered/refreshed by the DAG (`ensure_sql_table`), so new
  monthly partitions become visible **without restarting** the Thrift Server:
  the DAG runs `MSCK REPAIR TABLE` after each batch.
- The Thrift Server's embedded Hive metastore (Derby) lives on the named
  Docker volume `thrift_metastore_data` (mounted at
  `/opt/spark/work-dir`), so the table definition survives container restarts.

## Power BI Connection Expectations

Connect to:

```text
Server:   localhost
Port:     10000
Protocol: HiveServer2 / Spark Thrift (Spark SQL dialect)
JDBC:     jdbc:hive2://localhost:10000/default
```

In Power BI, use the **Spark connector** (Databricks connector is not
required; use the generic Spark/HiveServer2 datasource) and a query like:

```sql
SELECT * FROM default.curated_crimes
```

Because the table is partitioned by `year`/`month`, Power BI can also query a
single month (`WHERE year = 2001 AND month = 5`), which the Thrift Server
pushes down as partition pruning.

### How Power BI should refresh

Each monthly batch appends a new partition and runs `MSCK REPAIR TABLE`. Power
BI then just needs a **data refresh** to see the new month — no server restart
and no schema change. Old months stay visible and are never overwritten by new
runs.

### Power BI validation status

```text
Power BI client validation: NOT EXECUTED
Reason: Power BI is not installed/available.
Validated instead: Spark Thrift Server + SQL/JDBC boundary.
```

## Testing Without Power BI

The SQL boundary is verified end-to-end with beeline / JDBC:

```bash
# beeline (inside a container)
docker exec spark-thrift-server beeline \
  -u jdbc:hive2://localhost:10000 \
  -e 'SELECT COUNT(*) FROM default.curated_crimes;'

# partitions and per-month counts
docker exec airflow-scheduler beeline \
  -u jdbc:hive2://spark-thrift-server:10000 --silent=true \
  -e "SHOW PARTITIONS default.curated_crimes;
      SELECT year, month, COUNT(*) FROM default.curated_crimes
      GROUP BY year, month ORDER BY year, month;"

# any JDBC client from the host (verified with the bundled Hive JDBC driver):
#   jdbc:hive2://localhost:10000/default
```

The `verify_month` task in the monthly DAG performs the same visibility check
automatically after every run.

## Incremental Visibility (Simulated Power BI Refresh)

Processed months stay visible and new ones appear after their batch:

| Batch | Curated rows added | SQL total |
|---|---|---|
| 2001-01 | 37,853 | 37,853 |
| 2001-02 | 33,608 | 71,461 |
| 2001-03 | 40,339 | 111,800 |
| 2001-04 | 39,852 | 151,652 |
| 2001-05 | 41,579 | 193,231 |
| 2001-06 | 41,464 | 234,695 |
| 2001-07 | 44,370 | 279,065 |
| 2001-08 | 43,755 | 322,820 |

January remains queryable after February is added; no month is duplicated or
overwritten by another month's run.

## Performance

Measured on the same single-worker cluster (8 cores / 8 G):

| Metric | Old pipeline (2001-03) | New pipeline (2001-07) |
|---|---|---|
| Full DAG wall time | ~103 s | ~30 s |
| Data processing | ingest 74 s + DQ 24 s = 98 s | process_month 23 s |
| Rows scanned per run | 7,846,809 (whole CSV) | ~44,700 (one partition) |
| Bytes read per run | 1.85 GB (1,852,740,868 B) | ~3 MB (~610–670× less) |

The monthly DAG **never scans the 1.85 GB CSV**. It reads one Parquet
partition (~3 MB) via partition pruning, proven both by the direct partition
path in the read and by the `--explain` physical plan
(`PartitionFilters: [(year = …), (month = …)]`).

## Original Pipeline

The original implementation is preserved as a recoverable reference:

- DAG `chicago_crime_pipeline` (`airflow/dags/chicago_crime_pipeline.py`)
  — still present and functional (CSV → `/raw/YYYY-MM` → DQ → curated).
- App `spark/apps/process_crimes.py` — unchanged.

Do not run both DAGs for the same month at the same time (they write to the
same `/curated` partitions; both are overwrite-safe, but there is no point).

## Reset

```bash
docker compose down -v      # destroys ALL HDFS/DB/metastore state
docker compose up -d --build
docker exec airflow-scheduler airflow dags trigger chicago_crime_initial_load
```

Without `-v`, volumes (including the Thrift metastore) are preserved.

## Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| `Source partition missing: /source/crimes/year=…/month=…` | Initial load not run — trigger `chicago_crime_initial_load` once |
| `batch_month must be YYYY-MM` / `month out of range` | Invalid parameter; pass e.g. `{"batch_month":"2001-05"}` |
| `ensure_sql_table` fails | Thrift Server down/starting — restart the container and re-run the DAG (it self-heals) |
| New partition not visible in SQL | Run `MSCK REPAIR TABLE default.curated_crimes` (the DAG does this automatically) |
| `YearReported` column in source | Expected — renamed to avoid the `Year`/`year` case-insensitive clash; restored to `Year` during DQ |
| Table missing after Thrift restart | Should not happen (metastore volume). If it does, run `register_curated_crimes.sql` once |
| Curated partition has 4 part files after re-run | Expected — `overwrite` replaces the files of that partition only |

## Important

The following large files are **never committed to Git**:

```text
data/
airflow/docker/spark-3.5.1-bin-hadoop3.tgz
```
