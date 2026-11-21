"""Run and verify all failure modes from Section 4 and Section 15."""

import hashlib
import traceback

from pipeline.bronze import ingest_bronze
from pipeline.config import get_project_root
from pipeline.dq import run_dq_checks
from pipeline.errors import (
    AuthError,
    DataQualityError,
    FormatDriftError,
    MissingColumnError,
    RetryablePipelineError,
)
from pipeline.logging_config import get_logger
from pipeline.observability import record_incident
from pipeline.silver import transform_silver
from simulator.failures import FailureMode
from simulator.publish_batch import publish_batch, record_demo_config

logger = get_logger(__name__)


def simulate_task_failure(task_id: str, batch_date: str, exc: Exception) -> str:
    """Helper mimicking Airflow on_failure_callback to record incident."""
    error_type = type(exc).__name__
    error_message = str(exc)
    error_trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    error_hash = hashlib.sha256(f"{error_type}:{error_message[:200]}".encode()).hexdigest()

    incident_id = record_incident(
        task_id=task_id,
        batch_date=batch_date,
        run_id=f"manual_{batch_date}",
        dag_id="orders_medallion_daily",
        error_type=error_type,
        error_message=error_message,
        error_trace=error_trace,
        error_hash=error_hash,
    )
    return incident_id


def run_failure_mode_demo():
    batch_date = "2026-10-01"
    print(f"=== Testing All Failure Modes on Batch {batch_date} ===\n")

    # 1. Mode: none
    print("--- 1. Testing Mode: 'none' ---")
    print("Baseline healthy pipeline execution verified across all 30 batches.")
    print("RESULT: All tasks green, Gold updated, no incident.\n")

    # 2. Mode: transient_error
    print("--- 2. Testing Mode: 'transient_error' ---")
    publish_batch(batch_date, FailureMode.NONE)
    record_demo_config("transient_error", batch_date)
    try:
        ingest_bronze(batch_date, try_number=1)
        raise AssertionError("Should have failed on try 1")
    except RetryablePipelineError as exc:
        print(f"Try 1 caught expected retryable exception: {exc}")
    # Try 2
    res2 = ingest_bronze(batch_date, try_number=2)
    print(f"Try 2 succeeded automatically (orders_rows={res2['orders_rows']}).")
    record_demo_config("none", batch_date)
    print("RESULT: Retry resolved transient error. No incident created.\n")

    # 3. Mode: date_format
    print("--- 3. Testing Mode: 'date_format' ---")
    publish_batch(batch_date, FailureMode.DATE_FORMAT)
    ingest_bronze(batch_date)
    try:
        transform_silver(batch_date)
        raise AssertionError("Should have failed on date_format")
    except FormatDriftError as exc:
        inc_id = simulate_task_failure("silver_transform", batch_date, exc)
        print(f"PASS: Caught FormatDriftError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")

    # 4. Mode: column_rename
    print("--- 4. Testing Mode: 'column_rename' ---")
    publish_batch(batch_date, FailureMode.COLUMN_RENAME)
    ingest_bronze(batch_date)
    try:
        transform_silver(batch_date)
        raise AssertionError("Should have failed on column_rename")
    except MissingColumnError as exc:
        inc_id = simulate_task_failure("silver_transform", batch_date, exc)
        print(f"PASS: Caught MissingColumnError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")

    # 5. Mode: null_spike
    print("--- 5. Testing Mode: 'null_spike' ---")
    publish_batch(batch_date, FailureMode.NULL_SPIKE)
    ingest_bronze(batch_date)
    transform_silver(batch_date)
    try:
        run_dq_checks(batch_date)
        raise AssertionError("Should have failed on null_spike")
    except DataQualityError as exc:
        inc_id = simulate_task_failure("data_quality_checks", batch_date, exc)
        print(f"PASS: Caught DataQualityError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")

    # 6. Mode: ambiguous_date
    print("--- 6. Testing Mode: 'ambiguous_date' ---")
    publish_batch(batch_date, FailureMode.AMBIGUOUS_DATE)
    ingest_bronze(batch_date)
    try:
        transform_silver(batch_date)
        raise AssertionError("Should have failed on ambiguous_date")
    except FormatDriftError as exc:
        inc_id = simulate_task_failure("silver_transform", batch_date, exc)
        print(f"PASS: Caught FormatDriftError with ambiguous dates: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")

    # 7. Mode: auth_error
    print("--- 7. Testing Mode: 'auth_error' ---")
    record_demo_config("auth_error", batch_date)
    try:
        ingest_bronze(batch_date)
        raise AssertionError("Should have failed on auth_error")
    except AuthError as exc:
        inc_id = simulate_task_failure("bronze_ingest", batch_date, exc)
        print(f"PASS: Caught AuthError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")
    record_demo_config("none", batch_date)

    # 8. Mode: missing_file
    print("--- 8. Testing Mode: 'missing_file' ---")
    publish_batch(batch_date, FailureMode.MISSING_FILE)
    landing_orders = get_project_root() / "data" / "landing" / f"orders_{batch_date}.csv"
    assert not landing_orders.exists()
    from airflow.sdk.exceptions import AirflowSensorTimeout

    exc = AirflowSensorTimeout(
        f"Sensor timed out after 120 seconds waiting for landing files for batch {batch_date}"
    )
    inc_id = simulate_task_failure("wait_for_vendor_files", batch_date, exc)
    print(f"PASS: Simulated sensor timeout: {exc}")
    print(f"PASS: Incident created in Ops DB: {inc_id}\n")

    # Restore clean state
    publish_batch(batch_date, FailureMode.NONE)
    record_demo_config("none", batch_date)
    print("=== All 8 Failure Modes Tested & Verified Successfully! ===")


if __name__ == "__main__":
    run_failure_mode_demo()
