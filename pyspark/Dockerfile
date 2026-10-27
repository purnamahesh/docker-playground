FROM ubuntu:24.04

ARG SPARK_VERSION=4.2.0

WORKDIR /home/ubuntu

RUN apt-get update \
    && apt-get install -y wget openjdk-25-jre-headless sudo python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# REMOVE
COPY ./spark-4.2.0-bin-hadoop3.tgz /home/ubuntu/spark-4.2.0-bin-hadoop3.tgz

RUN tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark 

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON="python3"
RUN ln -s ${SPARK_HOME}/python/lib/py4j-*-src.zip ${SPARK_HOME}/python/lib/py4j-src.zip
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-src.zip"
ENV PATH="/opt/venv/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir jupyterlab

RUN usermod -aG sudo ubuntu
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && chown -R ubuntu:ubuntu /opt/venv \
    && chown -R ubuntu:ubuntu /opt/spark

USER ubuntu

WORKDIR /home/ubuntu/work_dir