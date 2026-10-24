FROM ubuntu:24.04

ARG SPARK_VERSION

WORKDIR /home/ubuntu

RUN apt-get update && apt-get install -y wget

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN apt-get install -y python3 openjdk-25-jdk sudo python3-pip

RUN tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt

RUN rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark 

WORKDIR /opt/spark 

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

RUN pip install pyspark