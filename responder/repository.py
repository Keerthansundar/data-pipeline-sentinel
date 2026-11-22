"""Postgres repository for ops database queries and audit persistence."""

import json
import os
from datetime import datetime
from typing import Any

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


class OpsRepository:
    """Read/write repository for ops Postgres database."""

    def __init__(self, db_url: str | None = None) -> None:
        self.db_url = db_url

    def _get_connection(self) -> Any:
        try:
            import psycopg

            if self.db_url:
                return psycopg.connect(self.db_url, connect_timeout=3)

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
            logger.warning("Could not connect to Ops database: %s", exc)
            return None

    def get_open_incidents(self) -> list[dict[str, Any]]:
        """Fetch open incidents requiring diagnosis."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, dag_id, task_id, run_id, batch_date,
                               error_type, error_message, error_trace, error_hash, created_at
                        FROM incidents
                        WHERE status = 'OPEN'
                        ORDER BY created_at ASC;
                        """
                    )
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, row, strict=False)) for row in rows]
        except Exception as exc:
            logger.error("Error fetching open incidents: %s", exc)
            return []

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        """Fetch a specific incident by its ID."""
        conn = self._get_connection()
        if not conn:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, dag_id, task_id, run_id, batch_date,
                               error_type, error_message, error_trace, error_hash, created_at
                        FROM incidents
                        WHERE id = %s;
                        """,
                        (incident_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        cols = [desc[0] for desc in cur.description]
                        return dict(zip(cols, row, strict=False))
                    return None
        except Exception as exc:
            logger.error("Error fetching incident %s: %s", incident_id, exc)
            return None

    def get_task_error_log(self, incident_id: str) -> dict[str, Any]:
        """Fetch error message, stack trace, and prior 50 log lines from pipeline_log."""
        inc = self.get_incident(incident_id)
        if not inc:
            return {
                "incident_id": incident_id,
                "error_message": f"No incident record found for {incident_id}",
                "traceback": "",
                "recent_logs": [],
            }

        conn = self._get_connection()
        recent_logs = []
        if conn:
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT logged_at, level, logger, message, context
                            FROM pipeline_log
                            WHERE batch_date = %s AND (task_id = %s OR task_id IS NULL)
                            ORDER BY logged_at ASC, id ASC
                            LIMIT 50;
                            """,
                            (
                                str(inc["batch_date"]) if inc.get("batch_date") is not None else "",
                                inc.get("task_id"),
                            ),
                        )
                        for r in cur.fetchall():
                            logged_at_str = r[0].isoformat() if r[0] else ""
                            recent_logs.append(f"{logged_at_str} [{r[1]}] {r[2]}: {r[3]}")
            except Exception as exc:
                logger.warning("Error fetching logs for incident %s: %s", incident_id, exc)

        return {
            "incident_id": incident_id,
            "task_id": inc.get("task_id", ""),
            "batch_date": inc.get("batch_date", ""),
            "error_type": inc.get("error_type", ""),
            "error_message": inc.get("error_message", ""),
            "traceback": inc.get("error_trace", ""),
            "recent_logs": recent_logs,
        }

    def get_run_history(self, task_id: str, limit: int = 10) -> list[dict[str, Any]]:
        """Fetch past run durations and statuses for a task."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT dag_run_id, task_id, batch_date, status,
                               rows_in, rows_out, rows_quarantined, error_type,
                               error_message, started_at, ended_at
                        FROM pipeline_run_log
                        WHERE task_id = %s
                        ORDER BY started_at DESC
                        LIMIT %s;
                        """,
                        (task_id, limit),
                    )
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, row, strict=False)) for row in rows]
        except Exception as exc:
            logger.error("Error fetching run history for %s: %s", task_id, exc)
            return []

    def get_column_profiles(self, batch_date: str) -> list[dict[str, Any]]:
        """Fetch column profiles for a specific batch date."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT batch_date, table_name, column_name, total_rows,
                               null_count, null_rate, distinct_count, patterns,
                               ambiguous_day_month_share
                        FROM column_profile
                        WHERE batch_date = %s
                        ORDER BY column_name;
                        """,
                        (batch_date,),
                    )
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, row, strict=False)) for row in rows]
        except Exception as exc:
            logger.error("Error fetching column profile for %s: %s", batch_date, exc)
            return []

    def get_schema_snapshot(
        self, batch_date: Any, table_name: str = "bronze.orders"
    ) -> dict[str, Any] | None:
        """Fetch schema snapshot for a specific batch date and table."""
        conn = self._get_connection()
        if not conn:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT columns
                        FROM schema_snapshot
                        WHERE batch_date = %s AND table_name = %s
                        ORDER BY created_at DESC
                        LIMIT 1;
                        """,
                        (batch_date, table_name),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        return json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    return None
        except Exception as exc:
            logger.error("Error fetching schema snapshot: %s", exc)
            return None

    def get_last_good_batch_date(self, current_batch_date: str) -> str | None:
        """Find the most recent batch date with successful Gold run prior to current."""
        conn = self._get_connection()
        if not conn:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT batch_date
                        FROM pipeline_run_log
                        WHERE batch_date < %s AND status = 'SUCCESS' AND task_id = 'gold_build'
                        ORDER BY batch_date DESC
                        LIMIT 1;
                        """,
                        (current_batch_date,),
                    )
                    row = cur.fetchone()
                    return str(row[0]) if row else None
        except Exception as exc:
            logger.error("Error finding last good batch date: %s", exc)
            return None

    def get_dq_results(self, batch_date: Any) -> list[dict[str, Any]]:
        """Fetch data quality check results for a specific batch date."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT table_name, check_name, observed_value,
                               threshold_value, passed, details, created_at
                        FROM dq_results
                        WHERE batch_date = %s
                        ORDER BY created_at ASC;
                        """,
                        (batch_date,),
                    )
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, row, strict=False)) for row in rows]
        except Exception as exc:
            logger.error("Error fetching DQ results for %s: %s", batch_date, exc)
            return []

    def record_tool_call(
        self,
        incident_id: str,
        step: int,
        tool_name: str,
        arguments: dict[str, Any],
        output_truncated: str,
        started_at: datetime,
        duration_ms: int,
    ) -> None:
        """Record an agent tool execution audit line."""
        conn = self._get_connection()
        if not conn:
            return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO incident_tool_calls (
                            incident_id, step, tool_name, arguments,
                            output_truncated, started_at, duration_ms
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            incident_id,
                            step,
                            tool_name,
                            json.dumps(arguments),
                            output_truncated,
                            started_at,
                            duration_ms,
                        ),
                    )
        except Exception as exc:
            logger.warning("Error recording tool call audit: %s", exc)

    def record_audit(
        self,
        incident_id: str,
        diagnosis: dict[str, Any],
        validation_results: dict[str, Any],
        policy_results: dict[str, Any],
        sandbox_results: dict[str, Any] | None,
        final_action: str,
        pr_url: str | None = None,
        email_status: str = "PENDING",
        model: str | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
    ) -> None:
        """Record final diagnosis, policy checks, sandbox, and routing audit."""
        conn = self._get_connection()
        if not conn:
            return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO incident_diagnosis_audit (
                            incident_id, diagnosis, validation_results, policy_results,
                            sandbox_results, final_action, pr_url_or_preview, email_status,
                            model, prompt_tokens, completion_tokens, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                        """,
                        (
                            incident_id,
                            json.dumps(diagnosis),
                            json.dumps(validation_results),
                            json.dumps(policy_results),
                            json.dumps(sandbox_results or {}),
                            final_action,
                            pr_url,
                            email_status,
                            model,
                            prompt_tokens,
                            completion_tokens,
                        ),
                    )
        except Exception as exc:
            logger.warning("Error recording diagnosis audit: %s", exc)

    def update_incident_status(self, incident_id: str, status: str) -> None:
        """Update incident status (OPEN, IN_PROGRESS, PR_OPENED, ESCALATED, RESOLVED)."""
        conn = self._get_connection()
        if not conn:
            return
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE incidents
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s;
                        """,
                        (status, incident_id),
                    )
        except Exception as exc:
            logger.warning("Error updating incident status for %s: %s", incident_id, exc)
