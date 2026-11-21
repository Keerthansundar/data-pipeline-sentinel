"""Smoke test dummy DAG for verifying Airflow 3 execution engine."""

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.logging_config import get_logger

logger = get_logger(__name__, dag_id="smoke_dummy_dag", task_id="smoke_task")


def _smoke_task():
    logger.info("Smoke dummy task executed successfully inside Airflow container")


with DAG(
    dag_id="smoke_dummy_dag",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    tags=["smoke", "checkpoint"],
) as dag:
    PythonOperator(
        task_id="smoke_task",
        python_callable=_smoke_task,
    )
