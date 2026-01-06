#!/bin/bash

# remember to make link of /opt/mssql-tools18/bin/sqlcmd into /usr/local/bin
# Args
#    -C for trust the server certificate
#    -l for timeout in sec
#    -S connection string
#    -h first n-th lines
#    -t query timeout
#    -U user name (pwd is passed via env var SQLCMDPASSWORD)
#    -Q query
sqlcmd\
    -C\
    -l 15\
    -S localhost\
    -h -1\
    -t 1\
    -U sa\
    -Q "SET NOCOUNT ON; Select SUM(state) from sys.databases"
