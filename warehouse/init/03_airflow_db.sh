#!/bin/bash
# Runs once, on the first start of an empty Postgres volume.
# Creates the separate database that Airflow uses for its own bookkeeping.
set -euo pipefail

: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD is not set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v pw="$AIRFLOW_DB_PASSWORD" <<'EOSQL'
CREATE USER airflow WITH PASSWORD :'pw';
CREATE DATABASE airflow OWNER airflow ENCODING 'UTF8';
EOSQL
