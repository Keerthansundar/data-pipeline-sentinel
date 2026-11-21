"""Vendor feed simulator DAG."""

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.logging_config import get_logger

logger = get_logger(__name__, dag_id="vendor_feed_simulator")


def _run_simulator(**context):
    logger.info("Vendor feed simulator triggered")


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
