FROM ubuntu:24.04

ARG SPARK_VERSION=4.2.0

WORKDIR /home/ubuntu

RUN apt-get update && apt-get install -y wget

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN apt-get install -y openjdk-25-jre-headless sudo ca-certificates \
        && rm -rf /var/lib/apt/lists/*

RUN tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark 

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON="python3"
RUN ln -s ${SPARK_HOME}/python/lib/py4j-*-src.zip ${SPARK_HOME}/python/lib/py4j-src.zip
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-src.zip"

RUN usermod -aG sudo ubuntu
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

