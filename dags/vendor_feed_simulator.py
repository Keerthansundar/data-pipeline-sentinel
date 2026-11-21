"""Vendor feed simulator DAG."""

import os
from datetime import UTC, datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.logging_config import get_logger
from simulator.failures import FailureMode
from simulator.publish_batch import publish_batch

logger = get_logger(__name__, dag_id="vendor_feed_simulator")


def get_demo_config_failure() -> str:
    """Fetch current failure mode configured in ops database.

    Returns:
        Configured failure mode string or 'none'.
    """
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
                    return str(row[0])
    except Exception:
        pass
    return FailureMode.NONE.value


def _run_simulator(**context: Any) -> None:
    """Publish batch files applying active failure mode."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    batch_date = conf.get("batch_date")
    if not batch_date:
        logical_date = context.get("logical_date")
        if logical_date:
            batch_date = logical_date.strftime("%Y-%m-%d")
        else:
            batch_date = datetime.now(UTC).strftime("%Y-%m-%d")

    failure = conf.get("failure")
    if not failure:
        failure = get_demo_config_failure()

    logger.info(
        "Simulator task executing: batch_date=%s, failure_mode=%s",
        batch_date,
        failure,
    )
    publish_batch(batch_date=batch_date, failure=FailureMode(failure))
    logger.info("Simulator task completed successfully for batch %s", batch_date)


with DAG(
    dag_id="vendor_feed_simulator",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    tags=["simulator", "demo"],
) as dag:
    simulate_task = PythonOperator(
        task_id="publish_vendor_feed",
        python_callable=_run_simulator,
    )
