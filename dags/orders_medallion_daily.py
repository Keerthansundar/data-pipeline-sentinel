"""Daily medallion pipeline DAG for orders and customers."""

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.logging_config import get_logger

logger = get_logger(__name__, dag_id="orders_medallion_daily")


def _noop_task(task_name: str):
    logger.info("Executing task: %s", task_name)


with DAG(
    dag_id="orders_medallion_daily",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 1 * * *",
    catchup=False,
    tags=["medallion", "production"],
) as dag:
    wait_for_vendor_files = PythonOperator(
        task_id="wait_for_vendor_files",
        python_callable=_noop_task,
        op_kwargs={"task_name": "wait_for_vendor_files"},
    )
    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=_noop_task,
        op_kwargs={"task_name": "bronze_ingest"},
    )
    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=_noop_task,
        op_kwargs={"task_name": "silver_transform"},
    )
    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=_noop_task,
        op_kwargs={"task_name": "data_quality_checks"},
    )
    gold_build = PythonOperator(
        task_id="gold_build",
        python_callable=_noop_task,
        op_kwargs={"task_name": "gold_build"},
    )
    freshness_check = PythonOperator(
        task_id="freshness_check",
        python_callable=_noop_task,
        op_kwargs={"task_name": "freshness_check"},
    )
    resolve_incidents = PythonOperator(
        task_id="resolve_incidents",
        python_callable=_noop_task,
        op_kwargs={"task_name": "resolve_incidents"},
    )

    (
        wait_for_vendor_files
        >> bronze_ingest
        >> silver_transform
        >> data_quality_checks
        >> gold_build
        >> freshness_check
        >> resolve_incidents
    )
