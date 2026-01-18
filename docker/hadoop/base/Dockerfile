FROM ubuntu:24.04

ARG JAVA_VERSION=${JAVA_VERSION:-8}
ARG HADOOP_VERSION=${HADOOP_VERSION:-3.4.2}

ARG DEBIAN_FRONTEND=noninteractive

ENV JAVA_VERSION=$JAVA_VERSION
ENV JAVA_HOME=/usr/lib/jvm/java-$JAVA_VERSION-openjdk-amd64/

RUN apt-get update && apt-get install -y --no-install-recommends \
    net-tools \
    curl \
    netcat-openbsd \
    gnupg \
    libsnappy-dev \
    nano \
    openjdk-$JAVA_VERSION-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN curl -O https://downloads.apache.org/hadoop/common/KEYS
RUN gpg --import KEYS

ENV HADOOP_VERSION=$HADOOP_VERSION
ENV HADOOP_URL=https://www.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

RUN set -x && \
    curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz && \
    curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc && \
    gpg --verify /tmp/hadoop.tar.gz.asc && \
    tar -xvf /tmp/hadoop.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop && \
    unset HADOOP_URL && \
    rm /tmp/hadoop.tar.gz*

RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs
RUN mkdir /hadoop-data

ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV PATH=$PATH:$HADOOP_HOME/bin/

ENV USER=root

ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
