FROM ubuntu:24.04

ENV ENABLE_INIT_DAEMON=false
ENV INIT_DAEMON_STEP=spark_master_init
ENV INIT_DAEMON_BASE_URI=http://identifier/init-daemon

ENV BASE_URL=https://downloads.apache.org/spark
ENV SPARK_VERSION=4.1.1
ENV HADOOP_VERSION=3

COPY wait-for-step.sh /
COPY execute-step.sh /
COPY finish-step.sh /

COPY bde-spark.css /css/org/apache/spark/ui/static/timeline-view.css

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    nano \
    openjdk-17-jdk \
    gnupg \
    python3 \
    python3-pip \
    coreutils procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

RUN curl -O "${BASE_URL}/KEYS"
RUN gpg --import KEYS

WORKDIR /tmp
RUN set -x && \
    curl -fSL "${BASE_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -o spark-bin-hadoop.tar.gz && \
    curl -fSL "${BASE_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz.asc" -o spark-bin-hadoop.tar.gz.asc && \
    gpg --verify spark-bin-hadoop.tar.gz.asc && \
    tar -xvf spark-bin-hadoop.tar.gz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    mv spark /opt/ && \
    rm spark-bin-hadoop.tar.gz*

WORKDIR /

RUN chmod +x /wait-for-step.sh && \
    chmod +x /execute-step.sh && \
    chmod +x /finish-step.sh

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED=1
