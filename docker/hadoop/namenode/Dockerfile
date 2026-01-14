FROM de/base-hadoop:1.0.0

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name

RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

EXPOSE 9000 9870

CMD ["/run.sh"]
