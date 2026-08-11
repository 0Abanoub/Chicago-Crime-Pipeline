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
