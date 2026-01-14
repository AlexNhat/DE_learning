FROM de/base-spark:1.0.0

COPY master.sh /

ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_MASTER_LOG=/opt/spark/logs

HEALTHCHECK CMD curl -f http://localhost:8080/ || exit 1

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/master.sh"]
