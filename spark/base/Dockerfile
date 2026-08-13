# Custom Spark base image for the Chicago Crime Pipeline.
#
# The upstream apache/spark image ships the full distribution at /opt/spark but
# does NOT put its bin/ dir on PATH and does not expose the legacy /spark path.
# This small layer fixes both so the cluster behaves identically to the classic
# bde2020 setup:
#   - /spark -> /opt/spark symlink
#   - /opt/spark/bin and /opt/spark/sbin on PATH
ARG SPARK_IMAGE_TAG=3.5.1
FROM apache/spark:${SPARK_IMAGE_TAG}

USER root

RUN ln -sfn /opt/spark /spark && \
    apt-get update && \
    apt-get install -y --no-install-recommends iproute2 net-tools procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}

USER spark
