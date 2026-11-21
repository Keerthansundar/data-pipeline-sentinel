"""Daily medallion pipeline DAG for orders and customers."""

import hashlib
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.sdk.exceptions import AirflowFailException

from pipeline.config import DEMO_MODE, get_project_root
from pipeline.errors import (
    AuthError,
    DataQualityError,
    FormatDriftError,
    MissingColumnError,
)
from pipeline.logging_config import get_logger
from pipeline.observability import record_incident, resolve_incidents_for_batch

logger = get_logger(__name__, dag_id="orders_medallion_daily")


def _get_batch_date(context: Any) -> str:
    """Extract batch_date from dag_run conf or logical_date."""
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and "batch_date" in dag_run.conf:
        return str(dag_run.conf["batch_date"])
    logical_date = context.get("logical_date")
    if logical_date:
        return logical_date.strftime("%Y-%m-%d")
    return datetime.now(UTC).strftime("%Y-%m-%d")


def on_failure_callback(context: Any) -> None:
    """Failure callback to record de-duplicated incidents into Ops DB."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = "orders_medallion_daily"
    run_id = context.get("dag_run").run_id if context.get("dag_run") else "manual"
    batch_date = _get_batch_date(context)

    exception = context.get("exception")
    error_type = type(exception).__name__ if exception else "PipelineFailure"
    error_message = str(exception) if exception else "Task failed"

    # Extract original underlying exception if wrapped in AirflowFailException
    if isinstance(exception, AirflowFailException) and exception.__cause__:
        error_type = type(exception.__cause__).__name__
        error_message = str(exception.__cause__)

    error_trace = (
        "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        if exception
        else ""
    )
    error_hash = hashlib.sha256(f"{error_type}:{error_message[:200]}".encode()).hexdigest()

    logger.error(
        "on_failure_callback triggered: task=%s, batch=%s, type=%s, message=%s",
        task_id,
        batch_date,
        error_type,
        error_message[:100],
    )

    record_incident(
        task_id=task_id,
        batch_date=batch_date,
        run_id=run_id,
        dag_id=dag_id,
        error_type=error_type,
        error_message=error_message,
        error_trace=error_trace,
        error_hash=error_hash,
    )


class VendorLandingFilesSensor(BaseSensorOperator):
    """File sensor verifying that daily vendor files exist in data/landing."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def poke(self, context: Any) -> bool:
        batch_date = _get_batch_date(context)
        landing_dir = get_project_root() / "data" / "landing"
        orders_file = landing_dir / f"orders_{batch_date}.csv"
        customers_file = landing_dir / f"customers_{batch_date}.json"

        orders_exist = orders_file.exists() and orders_file.stat().st_size > 0
        cust_exist = customers_file.exists() and customers_file.stat().st_size > 0

        logger.info(
            "Poking for vendor landing files (batch=%s): orders_ready=%s, customers_ready=%s",
            batch_date,
            orders_exist,
            cust_exist,
        )
        return bool(orders_exist and cust_exist)


def _run_bronze_task(**context: Any) -> None:
    from pipeline.bronze import ingest_bronze

    batch_date = _get_batch_date(context)
    ti = context.get("task_instance")
    try_number = ti.try_number if ti else 1
    run_id = context.get("dag_run").run_id if context.get("dag_run") else "manual"

    try:
        ingest_bronze(batch_date, try_number=try_number, dag_run_id=run_id, task_id="bronze_ingest")
    except AuthError as exc:
        raise AirflowFailException(str(exc)) from exc


def _run_silver_task(**context: Any) -> None:
    from pipeline.silver import transform_silver

    batch_date = _get_batch_date(context)
    run_id = context.get("dag_run").run_id if context.get("dag_run") else "manual"

    try:
        transform_silver(batch_date, dag_run_id=run_id, task_id="silver_transform")
    except (FormatDriftError, MissingColumnError) as exc:
        raise AirflowFailException(str(exc)) from exc


def _run_dq_task(**context: Any) -> None:
    from pipeline.dq import run_dq_checks

    batch_date = _get_batch_date(context)
    run_id = context.get("dag_run").run_id if context.get("dag_run") else "manual"

    try:
        run_dq_checks(batch_date, dag_run_id=run_id, task_id="data_quality_checks")
    except DataQualityError as exc:
        raise AirflowFailException(str(exc)) from exc


def _run_gold_task(**context: Any) -> None:
    from pipeline.gold import build_gold

    batch_date = _get_batch_date(context)
    run_id = context.get("dag_run").run_id if context.get("dag_run") else "manual"

    build_gold(batch_date, dag_run_id=run_id, task_id="gold_build")


def _run_freshness_task(**context: Any) -> None:
    from pipeline.spark_session import get_spark_session

    batch_date = _get_batch_date(context)
    gold_rev_path = get_project_root() / "data" / "gold" / "daily_revenue"

    if not gold_rev_path.exists():
        raise AirflowFailException(
            f"Freshness check failed: Gold daily_revenue table missing at {gold_rev_path}"
        )

    spark = get_spark_session("FreshnessCheck")
    rev_df = (
        spark.read.format("delta").load(str(gold_rev_path)).filter(f"_batch_date = '{batch_date}'")
    )
    count = rev_df.count()

    if count == 0:
        raise AirflowFailException(
            f"Freshness check failed: No rows in Gold for batch date {batch_date}"
        )

    logger.info(
        "Freshness check PASSED for batch_date=%s (found %d daily_revenue record)",
        batch_date,
        count,
    )


def _run_resolve_incidents_task(**context: Any) -> None:
    batch_date = _get_batch_date(context)
    resolved = resolve_incidents_for_batch(batch_date)
    logger.info("resolve_incidents completed for batch %s: %d resolved", batch_date, resolved)


# Retries and timeouts per Section 5
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10) if DEMO_MODE else timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=60) if DEMO_MODE else timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=5) if DEMO_MODE else timedelta(hours=1),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="orders_medallion_daily",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 1 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=15) if DEMO_MODE else timedelta(hours=3),
    tags=["medallion", "production"],
) as dag:
    wait_for_vendor_files = VendorLandingFilesSensor(
        task_id="wait_for_vendor_files",
        mode="reschedule",
        poke_interval=10 if DEMO_MODE else 300,
        timeout=120 if DEMO_MODE else 7200,
    )
    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=_run_bronze_task,
    )
    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=_run_silver_task,
    )
    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=_run_dq_task,
    )
    gold_build = PythonOperator(
        task_id="gold_build",
        python_callable=_run_gold_task,
    )
    freshness_check = PythonOperator(
        task_id="freshness_check",
        python_callable=_run_freshness_task,
    )
    resolve_incidents = PythonOperator(
        task_id="resolve_incidents",
        python_callable=_run_resolve_incidents_task,
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
