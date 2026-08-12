# Chicago Crime Data Engineering Pipeline

A fully local, Dockerized **batch data engineering pipeline** that turns the
Chicago Crime Dataset (1.85 GB CSV) into analytics-ready Parquet on HDFS and
serves it through Spark SQL to Power BI.

```
Source CSV ──► Airflow (one month per run, streamed)
                  │
                  ▼
              HDFS /raw/YYYY-MM
                  │
                  ▼
      Spark (processing + data quality)
                  │
                  ├──► HDFS /curated/year=YYYY/month=M  (Parquet)
                  └──► HDFS /rejected/YYYY-MM
                  │
            Hive table `curated_crimes` (external)
                  │
            Spark Thrift Server
                  │  JDBC :10000
                  ▼
               Power BI
```

Everything runs in one Docker network (`bigdata-net`) sized for a 16 GB
laptop. No cloud, no Kafka (batch data, not a stream), and no second database
warehouse — Spark is both the processing engine and the SQL serving layer.

## Architecture

| Layer | Technology | Responsibility |
|---|---|---|
| Source | `data/Crimes_-_2001_to_Present.csv` | immutable input (~7.8M rows, 22 cols) |
| Orchestration | Airflow (LocalExecutor) | schedules/tracks the batch workflow |
| Storage | HDFS | data lake: raw / curated / rejected zones |
| Processing | Apache Spark 3.5.1 (standalone) | schema enforcement, cleaning, data quality, Parquet writes |
| Serving | Spark Thrift Server | SQL endpoint on port `10000` for Power BI (JDBC/ODBC) |
| Infrastructure | Docker Compose | single-command reproducible stack |

## Service URLs

| Service | URL | Notes |
|---|---|---|
| HDFS NameNode UI | http://localhost:9870 | browse filesystem, live nodes |
| HDFS DataNode UI | http://localhost:9864 | |
| Spark Master UI | http://localhost:8080 | running/completed jobs |
| Spark Worker UI | http://localhost:8081 | |
| Spark Thrift (SQL) | `jdbc:hive2://localhost:10000` | Power BI / beeline |
| Airflow UI | http://localhost:8082 | `admin` / `admin` (change in `.env`) |

All host ports can be changed in `.env`.

## Quick start

```bash
docker compose up -d --build
```

First boot takes several minutes (image builds + Spark tarball + Airflow DB
migration). Check everything came up:

```bash
docker compose ps
```

## Running the pipeline

The DAG `chicago_crime_pipeline` is intentionally manual (`schedule=None`).
**One DAG run = one monthly batch.** The target month is the `batch_month`
DAG parameter (`YYYY-MM`):

1. Open Airflow: http://localhost:8082
2. Find **chicago_crime_pipeline** and hit the play button → *Trigger DAG*
3. Under *Configuration JSON* set `{"batch_month": "2001-01"}` (optional;
   defaults to `2001-01`)
4. Watch `ingest_raw_to_hdfs → spark_dq_and_curate → verify_curated_parquet`

Or trigger it from the CLI:

```bash
docker exec airflow-scheduler airflow dags trigger \
  --conf '{"batch_month":"2001-01"}' chicago_crime_pipeline
```

### What each task does

1. **ingest_raw_to_hdfs** — streams the ~1.85 GB source CSV line-by-line
   (never loading it into memory, never uploading the whole file) and writes
   ONLY the records whose crime date falls inside the target month to
   `/raw/2001-01/crime.csv` via WebHDFS. Rows with an unparseable date are
   counted and forwarded to the batch so the DQ step surfaces them as
   `INVALID_DATE` rather than dropping them silently.
2. **spark_dq_and_curate** — `SparkSubmitOperator` submits
   `spark/apps/process_crimes.py` to `spark://spark-master:7077` with the
   batch month as its argument. Spark reads `/raw/<batch_month>/crime.csv`,
   applies row-level data-quality checks, writes valid rows to `/curated`
   (Parquet, partitioned by `year` + `month`) and rejected rows to
   `/rejected/<batch_month>`.
3. **verify_curated_parquet** — batch-aware check that THIS month's raw file,
   curated partition (`year=<y>/month=<m>`) and rejected output actually exist
   and are non-empty, so an old batch can never mask a broken new one.

Reruns are idempotent: curated uses Spark *dynamic partition overwrite*, so
rerunning `2001-01` replaces only `year=2001/month=1` and never touches other
months; rejected output is scoped to `/rejected/<batch_month>`.

## Data lake zones

```
HDFS
├── /raw/2001-01/crime.csv        only that month's rows (header preserved)
├── /raw/2002-06/crime.csv        …one directory per batch
├── /curated/year=2001/month=1/   validated Parquet (year + month partitions)
├── /curated/year=2002/month=6/
└── /rejected/2001-01/            rows that failed data-quality checks, per batch
```

Inspect them from the host:

```bash
docker exec namenode hdfs dfs -ls -R /curated | head
docker exec namenode hdfs dfs -ls -h /raw/2001-01/
```

## Data quality checks (in `spark/apps/process_crimes.py`)

| Check | Rule |
|---|---|
| `ID_NULL` | every crime record must have an ID |
| `PRIMARY_TYPE_NULL` | crime type must be present |
| `INVALID_DATE` | `Date` must parse as `MM/dd/yyyy hh:mm:ss a` |
| `YEAR_MISMATCH` | year parsed from `Date` must equal the `Year` column |
| `NULL_GEO` | `Latitude`/`Longitude` must be present |
| `INVALID_LAT` / `INVALID_LON` | coordinates inside Chicago's bounding box |
| `DUPLICATE_ID` | duplicate `ID`s are rejected, one record kept per ID |

Records failing any check are written to `/rejected/<batch_month>` with a
`reject_reason` column. The job prints a row-level reconciliation
(`raw = curated + rejected`) on every run.

## Serving — Power BI via Spark Thrift

Power BI never touches HDFS directly. It connects to the Spark Thrift Server
over JDBC/ODBC:

```
Power BI ──► JDBC ──► Spark Thrift :10000 ──► Hive table `curated_crimes` ──► Parquet on HDFS
```

The curated Parquet is exposed as an **external Hive table** named
`curated_crimes` (partitioned by `year`/`month`), registered through the
Thrift Server with `spark/apps/register_curated_crimes.sql`. Re-register after
a Thrift Server restart (the embedded Derby metastore is not persistent):

```bash
docker exec spark-thrift-server beeline -u jdbc:hive2://localhost:10000 \
  -f /opt/spark-apps/register_curated_crimes.sql
```

Sanity checks with beeline:

```bash
docker exec spark-thrift-server beeline -u jdbc:hive2://localhost:10000 \
  -e 'SELECT count(*) FROM default.curated_crimes;'
docker exec spark-thrift-server beeline -u jdbc:hive2://localhost:10000 \
  -e 'SELECT `Primary Type`, count(*) n FROM default.curated_crimes GROUP BY `Primary Type` ORDER BY n DESC LIMIT 5;'
```

Two notes on the serving layer:

- The NameNode allows proxying for the thrift session users
  (`hadoop.proxyuser.{root,spark,anonymous}.*` in `hadoop/hadoop.env`) — the
  Thrift Server authenticates HDFS access with a proxy UGI even for the same
  user, so these rules are required or SQL fails with
  `User: root is not allowed to impersonate root`.
- Because the table points at the Parquet files directly, there is **no
  PostgreSQL warehouse** to keep in sync, and no second copy of the dataset.

## Why no Kafka?

The dataset is historical and arrives in batches, not as a real-time stream.
Kafka would add a streaming infrastructure that the business problem doesn't
need. If live events were ever ingested, Kafka could be added as an
ingestion layer in front of `/raw`.

## Resetting between sessions

```bash
docker compose down -v    # wipes HDFS + Postgres volumes — fresh start
docker compose up -d --build
```

Drop `-v` to keep data across sessions.

## Troubleshooting

- **Port already in use** — another stack (e.g. `../Last`) may be running on
  `9870/9864/7077/10000`. Stop it first, or change the port in `.env`.
- **Airflow webserver keeps restarting** — check `airflow-init` completed:
  `docker compose logs airflow-init`.
- **Spark job can't reach HDFS** — make sure you use `hdfs://namenode:9000/…`
  paths and that `./hadoop-conf` is mounted (containers talk by service name,
  never `localhost`).
- **Slow pip/`--no-deps` install** — the Airflow image skips the ~300 MB pip
  pyspark on purpose; the Spark distribution comes from the bundled tarball
  in `airflow/docker/`.
