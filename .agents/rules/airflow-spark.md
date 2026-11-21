# Airflow and PySpark Guidelines

## Airflow (v3.x LocalExecutor)
- Airflow runs in Docker using pinned version `apache/airflow:3.4.0` with `LocalExecutor`.
- Services: `postgres` (Airflow DB), `ops-postgres` (Ops DB), `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-init`, `mailpit`, `streamlit`.
- Task IDs are exact:
  - `wait_for_vendor_files` (file sensor, `mode="reschedule"`)
  - `bronze_ingest`
  - `silver_transform`
  - `data_quality_checks`
  - `gold_build`
  - `freshness_check`
  - `resolve_incidents`
- Failure Handling:
  - Deterministic errors (`FormatDriftError`, `MissingColumnError`, `DataQualityError`, `AuthError`) raise non-retryable Airflow exceptions to fail fast.
  - Transient errors raise standard exceptions and retry with exponential backoff.
  - `on_failure_callback` creates an incident in Postgres `incidents` table de-duplicated by `(task_id, batch_date, error_hash)`.

## PySpark & Delta Lake
- PySpark local mode configured in custom Airflow image with OpenJDK 17.
- Pinned compatibility: `pyspark==3.5.4` and `delta-spark==3.3.0`.
- Spark Session:
  - `spark.sql.extensions`: `io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog`: `org.apache.spark.sql.delta.catalog.DeltaCatalog`
  - Local mode resources: driver memory 2g, `spark.sql.shuffle.partitions` = 4.
- Partition overwrite:
  - Use Delta Lake `replaceWhere` on partition column `_batch_date` to guarantee idempotent reruns.
