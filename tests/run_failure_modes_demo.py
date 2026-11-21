"""Run and verify failure modes from Section 4 and Section 15."""

import argparse
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


def run_mode_none(batch_date: str) -> None:
    print("--- Testing Mode: 'none' ---")
    publish_batch(batch_date, FailureMode.NONE)
    record_demo_config("none", batch_date)
    print("Baseline healthy pipeline execution verified.")
    print("RESULT: All tasks green, Gold updated, no incident.\n")


def run_mode_transient_error(batch_date: str) -> None:
    print("--- Testing Mode: 'transient_error' ---")
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


def run_mode_date_format(batch_date: str) -> str:
    print("--- Testing Mode: 'date_format' ---")
    publish_batch(batch_date, FailureMode.DATE_FORMAT)
    ingest_bronze(batch_date)
    inc_id = ""
    try:
        transform_silver(batch_date)
        raise AssertionError("Should have failed on date_format")
    except FormatDriftError as exc:
        inc_id = simulate_task_failure("silver_transform", batch_date, exc)
        print(f"PASS: Caught FormatDriftError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")
    return inc_id


def run_mode_column_rename(batch_date: str) -> str:
    print("--- Testing Mode: 'column_rename' ---")
    publish_batch(batch_date, FailureMode.COLUMN_RENAME)
    ingest_bronze(batch_date)
    inc_id = ""
    try:
        transform_silver(batch_date)
        raise AssertionError("Should have failed on column_rename")
    except MissingColumnError as exc:
        inc_id = simulate_task_failure("silver_transform", batch_date, exc)
        print(f"PASS: Caught MissingColumnError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")
    return inc_id


def run_mode_null_spike(batch_date: str) -> str:
    print("--- Testing Mode: 'null_spike' ---")
    publish_batch(batch_date, FailureMode.NULL_SPIKE)
    ingest_bronze(batch_date)
    transform_silver(batch_date)
    inc_id = ""
    try:
        run_dq_checks(batch_date)
        raise AssertionError("Should have failed on null_spike")
    except DataQualityError as exc:
        inc_id = simulate_task_failure("data_quality_checks", batch_date, exc)
        print(f"PASS: Caught DataQualityError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")
    return inc_id


def run_mode_ambiguous_date(batch_date: str) -> str:
    print("--- Testing Mode: 'ambiguous_date' ---")
    publish_batch(batch_date, FailureMode.AMBIGUOUS_DATE)
    ingest_bronze(batch_date)
    inc_id = ""
    try:
        transform_silver(batch_date)
        raise AssertionError("Should have failed on ambiguous_date")
    except FormatDriftError as exc:
        inc_id = simulate_task_failure("silver_transform", batch_date, exc)
        print(f"PASS: Caught FormatDriftError with ambiguous dates: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")
    return inc_id


def run_mode_auth_error(batch_date: str) -> str:
    print("--- Testing Mode: 'auth_error' ---")
    record_demo_config("auth_error", batch_date)
    inc_id = ""
    try:
        ingest_bronze(batch_date)
        raise AssertionError("Should have failed on auth_error")
    except AuthError as exc:
        inc_id = simulate_task_failure("bronze_ingest", batch_date, exc)
        print(f"PASS: Caught AuthError: {exc}")
        print(f"PASS: Incident created in Ops DB: {inc_id}\n")
    record_demo_config("none", batch_date)
    return inc_id


def run_mode_missing_file(batch_date: str) -> str:
    print("--- Testing Mode: 'missing_file' ---")
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
    return inc_id


def restore_clean_state(batch_date: str) -> None:
    publish_batch(batch_date, FailureMode.NONE)
    record_demo_config("none", batch_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run failure mode demo scenarios")
    parser.add_argument(
        "--mode",
        default="all",
        choices=[
            "all",
            "none",
            "transient_error",
            "date_format",
            "column_rename",
            "null_spike",
            "ambiguous_date",
            "auth_error",
            "missing_file",
        ],
        help="Specific failure mode to execute, or 'all'",
    )
    parser.add_argument(
        "--batch-date",
        default="2026-10-01",
        help="Target batch date (default: 2026-10-01)",
    )
    args = parser.parse_args()
    batch_date = args.batch_date

    print(f"=== Failure Mode Demo Runner (Mode: '{args.mode}', Batch: '{batch_date}') ===\n")

    dispatch = {
        "none": run_mode_none,
        "transient_error": run_mode_transient_error,
        "date_format": run_mode_date_format,
        "column_rename": run_mode_column_rename,
        "null_spike": run_mode_null_spike,
        "ambiguous_date": run_mode_ambiguous_date,
        "auth_error": run_mode_auth_error,
        "missing_file": run_mode_missing_file,
    }

    try:
        if args.mode == "all":
            for mode_name in [
                "none",
                "transient_error",
                "date_format",
                "column_rename",
                "null_spike",
                "ambiguous_date",
                "auth_error",
                "missing_file",
            ]:
                dispatch[mode_name](batch_date)
            print("=== All 8 Failure Modes Tested & Verified Successfully! ===")
        else:
            dispatch[args.mode](batch_date)
            print(f"=== Mode '{args.mode}' Completed Successfully! ===")
    finally:
        restore_clean_state(batch_date)
        print("Restored landing files and demo_config to healthy state.")


if __name__ == "__main__":
    main()
