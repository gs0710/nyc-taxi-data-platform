-- Runs once, the first time the Postgres container starts with an empty data volume.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA staging IS 'Cleaned trips as loaded from Parquet (Phase 7)';
COMMENT ON SCHEMA marts   IS 'Star schema built by dbt (Phase 8)';
COMMENT ON SCHEMA audit   IS 'Load history and run metadata';
