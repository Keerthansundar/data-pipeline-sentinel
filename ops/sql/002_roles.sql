-- ============================================================================
-- Read-Only Role for Dashboard Queries on Airflow Metadata Database
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_ro') THEN
        CREATE ROLE airflow_ro WITH LOGIN PASSWORD 'airflow_ro_password';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE airflow TO airflow_ro;
GRANT USAGE ON SCHEMA public TO airflow_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO airflow_ro;
