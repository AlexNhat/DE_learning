ARG MSSQL_RELEASE_YEAR=2022
ARG DEBIAN_FRONTEND=noninteractive

FROM mcr.microsoft.com/mssql/server:${MSSQL_RELEASE_YEAR}-latest

USER root

COPY ./northwind.sql /usr/config/northwind.sql

COPY ./entrypoint.sh /usr/config/entrypoint.sh
COPY ./checkhealth.sh /usr/config/checkhealth.sh
COPY ./configure-db.sh /usr/config/configure-db.sh


RUN chmod +x /usr/config/entrypoint.sh
RUN chmod +x /usr/config/checkhealth.sh
RUN chmod +x /usr/config/configure-db.sh

EXPOSE 1433
ENTRYPOINT ["/usr/bin/sh", "/usr/config/entrypoint.sh"]
