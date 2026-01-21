FROM de/base-hadoop:1.0.0

ADD run.sh /run.sh
RUN chmod a+x /run.sh

HEALTHCHECK CMD curl -f http://localhost:8088/ || exit 1

EXPOSE 8088

CMD ["/run.sh"]
