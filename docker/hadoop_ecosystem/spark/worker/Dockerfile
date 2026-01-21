FROM de/base-spark:1.0.0

COPY worker.sh /

ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG=/opt/spark/logs
ENV SPARK_MASTER="spark://spark-master:7077"

HEALTHCHECK CMD curl -f http://localhost:8081/ || exit 1

EXPOSE 8081

CMD ["/bin/bash", "/worker.sh"]
