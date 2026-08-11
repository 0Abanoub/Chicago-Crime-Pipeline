ARG PYSPARK_VERSION=3.3.0
FROM jupyter/base-notebook:python-3.10

ARG PYSPARK_VERSION
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}
RUN pip install --no-cache-dir \
    pyspark==${PYSPARK_VERSION} \
    kafka-python==2.0.2 \
    hdfs==2.7.3 \
    pandas \
    matplotlib

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
