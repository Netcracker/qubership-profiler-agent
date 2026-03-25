#!/bin/bash

RETRY_ATTEMPTS=${MAX_RETRIES:-10}
until psql -U "${PG_TEST_USER}" -d "${PG_TEST_DATABASE}" -w -c "SELECT '${PG_TEST_DATABASE} is available'" 2>&1 || [ "${RETRY_ATTEMPTS}" -eq 0 ]; do
    echo "Waiting for postgres DB '${PG_TEST_DATABASE}' to accept connections, $((RETRY_ATTEMPTS--)) attempt(s) remain..."
    sleep "${POLL_INTERVAL:-3s}"
done
