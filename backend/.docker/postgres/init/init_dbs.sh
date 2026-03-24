#!/bin/sh

set -e

psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<-SQL
    CREATE USER ${PG_TEST_USER} WITH PASSWORD '${PG_TEST_USER_PASSWORD}';
    ALTER USER ${PG_TEST_USER} WITH SUPERUSER;
    CREATE DATABASE ${PG_TEST_DATABASE};
    GRANT ALL PRIVILEGES ON DATABASE ${PG_TEST_DATABASE} TO ${PG_TEST_USER};
SQL

ls /opt/apptest
ls /opt/apptest/bin

psql -U "${POSTGRES_USER}" -d "${PG_TEST_DATABASE}" -f /opt/apptest/bin/cdt_schema.sql
