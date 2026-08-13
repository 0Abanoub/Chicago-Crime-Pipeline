ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.9.3-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

USER root

# Java 17 is required to run the bundled Spark 3.5.1 distribution (spark-submit)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Full Spark distribution so the Airflow container can act as a Spark client
# (spark-submit) exactly like the cluster image. Spark 3.5.1 ships its own
# Hadoop 3 client jars, which is all the pipeline needs to read/write HDFS.
COPY spark-3.5.1-bin-hadoop3.tgz /tmp/spark-3.5.1-bin-hadoop3.tgz
RUN tar xzf /tmp/spark-3.5.1-bin-hadoop3.tgz -C /opt/ && \
    rm /tmp/spark-3.5.1-bin-hadoop3.tgz && \
    ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

USER airflow

# --no-deps avoids pulling the huge pip pyspark package: SparkSubmitOperator
# only shells out to the spark-submit we just unpacked.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --no-deps --default-timeout=180 --retries 8 -r /requirements.txt
