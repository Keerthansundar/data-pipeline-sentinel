"""Bronze layer raw ingestion."""

import os
from datetime import UTC, datetime
from typing import Any

from pyspark.sql.functions import current_timestamp, lit

from pipeline.config import get_project_root
from pipeline.errors import AuthError, RetryablePipelineError
from pipeline.logging_config import get_logger
from pipeline.observability import record_pipeline_run, record_schema_snapshot
from pipeline.spark_session import get_spark_session

logger = get_logger(__name__)


def check_demo_failure_mode(batch_date: str) -> str:
    """Check active demo failure mode for the target batch."""
    try:
        import psycopg

        db_host = os.getenv("OPS_DB_HOST", "ops-postgres")
        db_port = os.getenv("OPS_DB_PORT", "5432")
        db_name = os.getenv("OPS_DB_NAME", "ops")
        db_user = os.getenv("OPS_DB_USER", "ops_user")
        db_password = os.getenv("OPS_DB_PASSWORD", "ops_password")

        conn_str = (
            f"host={db_host} port={db_port} dbname={db_name} "
            f"user={db_user} password={db_password} connect_timeout=2"
        )
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT failure_mode, target_batch_date FROM demo_config WHERE id = 1;")
                row = cur.fetchone()
                if row and row[0]:
                    mode = str(row[0])
                    target_date = str(row[1]) if row[1] else None
                    if target_date is None or target_date == batch_date:
                        return mode
    except Exception:
        pass
    return "none"


def ingest_bronze(
    batch_date: str,
    try_number: int = 1,
    dag_run_id: str = "manual",
    task_id: str = "bronze_ingest",
) -> dict[str, Any]:
    """Ingest raw landing files into Bronze Delta tables.

    Args:
        batch_date: Batch date string in YYYY-MM-DD format.
        try_number: Task try number for retry handling.
        dag_run_id: Airflow DagRun ID.
        task_id: Task name.

    Returns:
        Summary dict containing rows ingested and metadata.
    """
    start_time = datetime.now(UTC)
    root = get_project_root()
    landing_dir = root / "data" / "landing"
    bronze_dir = root / "data" / "bronze"

    orders_file = landing_dir / f"orders_{batch_date}.csv"
    customers_file = landing_dir / f"customers_{batch_date}.json"

    logger.info(
        "Starting Bronze ingest: batch_date=%s, try_number=%d, inputs=[%s, %s]",
        batch_date,
        try_number,
        orders_file.name,
        customers_file.name,
    )

    # 1. Failure mode checks
    active_failure = check_demo_failure_mode(batch_date)
    if active_failure == "auth_error":
        logger.error(
            "Authentication to vendor source failed: 401 Unauthorized (batch=%s)",
            batch_date,
        )
        record_pipeline_run(
            dag_run_id,
            task_id,
            batch_date,
            "FAILED",
            error_type="AuthError",
            error_message="Failed to authenticate to vendor source: 401 Unauthorized",
            started_at=start_time,
        )
        raise AuthError("Failed to authenticate to vendor source: 401 Unauthorized")

    if active_failure == "transient_error" and try_number <= 1:
        logger.warning(
            "Transient network error reading vendor feed on try %d. Retrying...",
            try_number,
        )
        record_pipeline_run(
            dag_run_id,
            task_id,
            batch_date,
            "UP_FOR_RETRY",
            error_type="RetryablePipelineError",
            error_message="Transient network glitch reading vendor feed",
            started_at=start_time,
        )
        raise RetryablePipelineError("Transient network glitch reading vendor feed (try 1)")

    if not orders_file.exists() or not customers_file.exists():
        msg = f"Missing landing file(s) for batch {batch_date} in {landing_dir}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    spark = get_spark_session("BronzeIngest")

    # 2. Ingest Orders CSV
    orders_size_bytes = orders_file.stat().st_size
    orders_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(str(orders_file))
        .withColumn("_batch_date", lit(batch_date))
        .withColumn("_source_file", lit(orders_file.name))
        .withColumn("_ingested_at", current_timestamp())
        .coalesce(1)
    ).cache()

    orders_count = orders_df.count()
    orders_bronze_path = bronze_dir / "orders"
    orders_bronze_path.mkdir(parents=True, exist_ok=True)

    orders_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
        "replaceWhere", f"_batch_date = '{batch_date}'"
    ).save(str(orders_bronze_path))

    # Snapshot schema
    schema_dict = {f.name: str(f.dataType) for f in orders_df.schema.fields}
    record_schema_snapshot(batch_date, "bronze.orders", schema_dict)
    orders_df.unpersist()

    # 3. Ingest Customers JSON
    customers_size_bytes = customers_file.stat().st_size
    customers_df = (
        spark.read.json(str(customers_file))
        .withColumn("_batch_date", lit(batch_date))
        .withColumn("_source_file", lit(customers_file.name))
        .withColumn("_ingested_at", current_timestamp())
        .coalesce(1)
    ).cache()

    customers_count = customers_df.count()
    cust_bronze_path = bronze_dir / "customers"
    cust_bronze_path.mkdir(parents=True, exist_ok=True)

    customers_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
        "replaceWhere", f"_batch_date = '{batch_date}'"
    ).save(str(cust_bronze_path))

    cust_schema_dict = {f.name: str(f.dataType) for f in customers_df.schema.fields}
    record_schema_snapshot(batch_date, "bronze.customers", cust_schema_dict)
    customers_df.unpersist()

    end_time = datetime.now(UTC)
    duration_s = (end_time - start_time).total_seconds()

    logger.info(
        "Bronze ingest completed: batch=%s, orders_rows=%d (%d bytes), customers_rows=%d (%d bytes), duration=%.2fs",
        batch_date,
        orders_count,
        orders_size_bytes,
        customers_count,
        customers_size_bytes,
        duration_s,
    )
    logger.info(
        "Bronze customer rows ingested: count=%d, bytes=%d (batch=%s)",
        customers_count,
        customers_size_bytes,
        batch_date,
    )

    record_pipeline_run(
        dag_run_id=dag_run_id,
        task_id=task_id,
        batch_date=batch_date,
        status="SUCCESS",
        rows_in=orders_count,
        rows_out=orders_count,
        started_at=start_time,
        ended_at=end_time,
    )

    return {
        "batch_date": batch_date,
        "orders_rows": orders_count,
        "customers_rows": customers_count,
        "duration_seconds": duration_s,
    }
