# Install library
FROM apache/airflow:2.7.1-python3.9

USER root

RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean
# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

RUN pip install apache-airflow-providers-apache-spark pyspark==3.4 vnstock minio