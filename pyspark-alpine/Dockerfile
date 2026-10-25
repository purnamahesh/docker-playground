FROM alpine:3.24.3

ARG SPARK_VERSION=4.2.0

WORKDIR /opt

RUN apk update \
    && apk add python3 openjdk21-jre-headless wget tar sudo bash

# REMOVE
COPY ./spark-4.2.0-bin-hadoop3.tgz /opt/spark-4.2.0-bin-hadoop3.tgz

# RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop3 spark

ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN ln -s ${SPARK_HOME}/python/lib/py4j-*-src.zip ${SPARK_HOME}/python/lib/py4j-src.zip

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-src.zip"

RUN adduser -D -s /bin/bash icarus \
    && echo 'icarus ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/icarus \
    && chmod 0440 /etc/sudoers.d/icarus
USER icarus