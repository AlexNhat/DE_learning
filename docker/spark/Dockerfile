# Author: Apache Spark
FROM spark:4.0.1-scala2.13-java21-ubuntu

USER root

#COPY ./entrypoint.sh /opt/entrypoint.sh

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*


ENV SPARK_MODE=master
ENV SPARK_RPC_AUTHENTICATION_ENABLED=no
ENV SPARK_RPC_ENCRYPTION_ENABLED=no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
ENV SPARK_SSL_ENABLED=no
ENV SPARK_USER=spark

#USER spark

COPY entrypoint.sh .

#ENTRYPOINT ["entrypoint.sh"]

#CMD ["/opt/entrypoint.sh"]
