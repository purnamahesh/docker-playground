# ---------- Stage 1: unpack Spark (thrown away) ----------
FROM ubuntu:24.04 AS spark-dl
ARG SPARK_VERSION=4.2.0
# RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
#    && mv spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/spark.tgz
COPY spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/spark.tgz
RUN mkdir -p /opt/spark \
    && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm -rf /opt/spark/examples /opt/spark/data /opt/spark/R \
    && cd /opt/spark/python/lib && ln -s py4j-*-src.zip py4j-src.zip

# ---------- Stage 2: base ----------
FROM ubuntu:24.04 AS base

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         openjdk-25-jre-headless python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PIP_NO_CACHE_DIR=1
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-src.zip" \
    PATH="/opt/venv/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# chown in the SAME layer as creation -> no duplication
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install pyarrow \
    && chown -R ubuntu:ubuntu /opt/venv

# ownership set during copy -> no duplication
COPY --from=spark-dl --chown=ubuntu:ubuntu /opt/spark /opt/spark

USER ubuntu
WORKDIR /home/ubuntu/work_dir

# ---------- Stage 3: jupyter ----------
FROM base AS jupyter
RUN pip install jupyterlab boto3 fg-data-profiling ipywidgets sparkmeasure
