ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.9.3-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

USER root
# Java is required for pyspark's bundled spark-submit/JVM to run
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --default-timeout=180 --retries 8 -r /requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/default-java
# pyspark pip package ships its own spark-submit; expose it on PATH
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH
