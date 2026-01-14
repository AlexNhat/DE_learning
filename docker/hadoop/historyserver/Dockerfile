FROM de/base-hadoop:1.0.0

ENV YARN_CONF_yarn_timeline___service_leveldb___timeline___store_path=/hadoop/yarn/timeline

RUN mkdir -p /hadoop/yarn/timeline
VOLUME /hadoop/yarn/timeline

ADD run.sh /run.sh
RUN chmod a+x /run.sh

HEALTHCHECK CMD curl -f http://localhost:8188/ || exit 1

EXPOSE 8188

CMD ["/run.sh"]
