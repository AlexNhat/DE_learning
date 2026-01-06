#!/bin/bash

# Wait 60 seconds for SQL Server to start up by ensuring that 
# calling SQLCMD does not return an error code, which will ensure that sqlcmd is accessible
# and that system and user databases return "0" which means all databases are in an "online" state
# https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql?view=sql-server-2017 

#TODO: check for prev versions at /opt/mssql-tools/bin/sqlcmd 
if [[ ${MSSQL_RELEASE_YEAR} -eq 2022 ]]; then
    ln -fs /opt/mssql-tools18/bin/sqlcmd /usr/local/bin
fi


DBSTATUS=1
ERRCODE=1

while [[ $DBSTATUS -ne 0 ]] && [[ $ERRCODE -ne 0 ]]; do
	DBSTATUS=$(./usr/config/checkhealth.sh)
	ERRCODE=$?
done


if [[ $DBSTATUS -ne 0 ]] && [[ $ERRCODE -ne 0 ]]; then 
	echo "SQL Server took more than 60 seconds to start up or one or more databases are not in an ONLINE state"
	exit 1
fi


# Add new non-root user
sqlcmd -C -S localhost -U sa \
<<-EOSQL
    CREATE LOGIN $MSSQL_USER_NAME WITH PASSWORD = '$MSSQL_USER_PASSWORD';

    USE master;
    CREATE USER $MSSQL_USER_NAME FOR LOGIN $MSSQL_USER_NAME;
    ALTER ROLE db_owner ADD MEMBER $MSSQL_USER_NAME;
EOSQL

if [[ $? -eq 0 ]]; then
    echo "Added new user"
else
    echo "Failed to add new user"
fi


# Spawn Northwind db (if have)
if [[ ${INIT_NORTHWIND} = true ]]; then
    sqlcmd\
	-C\
	-l 15\
	-S localhost\
	-U sa\
	-d master\
	-i ./usr/config/northwind.sql

    if [[ $? -eq 0 ]]; then
        echo "Init Northwind"
    else
        echo "Failed to init Northwind"
    fi
fi

echo "Init process complete"
