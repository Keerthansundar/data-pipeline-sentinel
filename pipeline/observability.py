"""Observability logging, metrics, and incident database helpers."""

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def _get_db_connection():
    """Create a psycopg connection to the ops database.

    Returns:
        psycopg connection object, or None if connection fails.
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
            f"user={db_user} password={db_password} connect_timeout=3"
        )
        return psycopg.connect(conn_str)
    except Exception as exc:
        logger.warning("Failed to connect to Ops DB: %s", exc)
        return None


def record_pipeline_run(
    dag_run_id: str,
    task_id: str,
    batch_date: str,
    status: str,
    rows_in: int = 0,
    rows_out: int = 0,
    rows_quarantined: int = 0,
    error_type: str | None = None,
    error_message: str | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> None:
    """Record task execution summary in pipeline_run_log table."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_run_log (
                        dag_run_id, task_id, batch_date, status,
                        rows_in, rows_out, rows_quarantined,
                        error_type, error_message, started_at, ended_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        dag_run_id,
                        task_id,
                        batch_date,
                        status,
                        rows_in,
                        rows_out,
                        rows_quarantined,
                        error_type,
                        error_message[:2000] if error_message else None,
                        started_at or datetime.now(UTC),
                        ended_at or datetime.now(UTC),
                    ),
                )
    except Exception as exc:
        logger.warning("Error recording pipeline run log: %s", exc)


def record_incident(
    task_id: str,
    batch_date: str,
    run_id: str,
    dag_id: str,
    error_type: str,
    error_message: str,
    error_trace: str | None = None,
    error_hash: str | None = None,
) -> str | None:
    """Insert or de-duplicate an incident in the incidents table.

    Returns:
        incident_id string if created/existing, or None on failure.
    """
    if not error_hash:
        error_hash = hashlib.sha256(f"{error_type}:{error_message[:200]}".encode()).hexdigest()

    incident_id = f"INC-{batch_date}-{task_id}-{error_hash[:8]}"

    conn = _get_db_connection()
    if not conn:
        return None

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO incidents (
                        id, status, dag_id, task_id, run_id, batch_date,
                        error_type, error_message, error_trace, error_hash,
                        created_at, updated_at
                    ) VALUES (%s, 'OPEN', %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (task_id, batch_date, error_hash) DO NOTHING;
                    """,
                    (
                        incident_id,
                        dag_id,
                        task_id,
                        run_id,
                        batch_date,
                        error_type,
                        error_message[:4000],
                        error_trace[:8000] if error_trace else None,
                        error_hash,
                    ),
                )
        logger.info(
            "Incident recorded: id=%s, task=%s, batch=%s, type=%s, hash=%s",
            incident_id,
            task_id,
            batch_date,
            error_type,
            error_hash[:8],
        )
        return incident_id
    except Exception as exc:
        logger.warning("Error recording incident: %s", exc)
        return None


def resolve_incidents_for_batch(batch_date: str) -> int:
    """Mark OPEN and PR_OPENED incidents for this batch date as RESOLVED."""
    conn = _get_db_connection()
    if not conn:
        return 0

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE incidents
                    SET status = 'RESOLVED', updated_at = CURRENT_TIMESTAMP
                    WHERE batch_date = %s AND status IN ('OPEN', 'PR_OPENED');
                    """,
                    (batch_date,),
                )
                resolved_count = cur.rowcount
                logger.info(
                    "Resolved %d incident(s) for batch_date=%s",
                    resolved_count,
                    batch_date,
                )
                return resolved_count
    except Exception as exc:
        logger.warning("Error resolving incidents: %s", exc)
        return 0


def record_schema_snapshot(batch_date: str, table_name: str, columns: dict[str, str]) -> None:
    """Save schema snapshot for a table and batch date."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO schema_snapshot (batch_date, table_name, columns, created_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP);
                    """,
                    (batch_date, table_name, json.dumps(columns)),
                )
    except Exception as exc:
        logger.warning("Error recording schema snapshot: %s", exc)


def record_column_profile(
    batch_date: str,
    table_name: str,
    column_name: str,
    total_rows: int,
    null_count: int,
    null_rate: float,
    distinct_count: int | None = None,
    patterns: dict[str, Any] | None = None,
    ambiguous_day_month_share: float = 0.0,
) -> None:
    """Save column profile record into column_profile table."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO column_profile (
                        batch_date, table_name, column_name, total_rows,
                        null_count, null_rate, distinct_count, patterns,
                        ambiguous_day_month_share, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """,
                    (
                        batch_date,
                        table_name,
                        column_name,
                        total_rows,
                        null_count,
                        null_rate,
                        distinct_count,
                        json.dumps(patterns or {}),
                        ambiguous_day_month_share,
                    ),
                )
    except Exception as exc:
        logger.warning("Error recording column profile: %s", exc)


def record_dq_result(
    batch_date: str,
    table_name: str,
    check_name: str,
    observed_value: str,
    threshold_value: str,
    passed: bool,
    details: str | None = None,
) -> None:
    """Save data quality check result into dq_results table."""
    conn = _get_db_connection()
    if not conn:
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dq_results (
                        batch_date, table_name, check_name,
                        observed_value, threshold_value, passed, details, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """,
                    (
                        batch_date,
                        table_name,
                        check_name,
                        str(observed_value),
                        str(threshold_value),
                        passed,
                        details,
                    ),
                )
    except Exception as exc:
        logger.warning("Error recording DQ result: %s", exc)
