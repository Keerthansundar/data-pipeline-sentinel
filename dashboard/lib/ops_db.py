"""Queries for Ops database tables."""

import os
from datetime import date
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


class OpsDbAdapter:
    """Queries for pipeline_run_log, pipeline_log, and incidents."""

    def __init__(self, db_url: str | None = None):
        if db_url:
            self.db_url = db_url
        else:
            host = os.getenv("OPS_DB_HOST", "localhost")
            port = os.getenv("OPS_DB_PORT", "5434")
            dbname = os.getenv("OPS_DB_NAME", "ops")
            user = os.getenv("OPS_DB_USER", "ops_user")
            password = os.getenv("OPS_DB_PASSWORD", "ops_password")
            self.db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    def _get_connection(self) -> psycopg.Connection | None:
        try:
            return psycopg.connect(self.db_url, row_factory=dict_row, connect_timeout=3)
        except Exception as exc:
            logger.warning("Could not connect to Ops database: %s", exc)
            return None

    def get_recent_logs(
        self,
        dag_id: str | None = None,
        task_id: str | None = None,
        batch_date: str | None = None,
        level: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Query recent logs from pipeline_log table."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    query = "SELECT logged_at, level, logger, dag_id, task_id, run_id, batch_date, try_number, message FROM pipeline_log WHERE 1=1"
                    params: list[Any] = []
                    if dag_id and dag_id != "ALL":
                        query += " AND dag_id = %s"
                        params.append(dag_id)
                    if task_id and task_id != "ALL":
                        query += " AND task_id = %s"
                        params.append(task_id)
                    if batch_date:
                        query += " AND batch_date = %s"
                        params.append(batch_date)
                    if level and level != "ALL":
                        query += " AND level = %s"
                        params.append(level)
                    query += " ORDER BY logged_at DESC LIMIT %s;"
                    params.append(limit)
                    cur.execute(query, tuple(params))
                    return cur.fetchall()
        except Exception as exc:
            logger.warning("Error querying pipeline_log: %s", exc)
            return []

    def get_incidents(
        self, status: str | None = None, batch_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Query incidents from incidents table."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    query = "SELECT id, status, dag_id, task_id, run_id, batch_date, error_type, error_message, created_at, updated_at FROM incidents WHERE 1=1"
                    params: list[Any] = []
                    if status and status != "ALL":
                        query += " AND status = %s"
                        params.append(status)
                    if batch_date:
                        query += " AND batch_date = %s"
                        params.append(batch_date)
                    query += " ORDER BY created_at DESC;"
                    cur.execute(query, tuple(params))
                    return cur.fetchall()
        except Exception as exc:
            logger.warning("Error querying incidents: %s", exc)
            return []

    def get_incident_detail(self, incident_id: str) -> dict[str, Any] | None:
        """Query single incident by ID."""
        conn = self._get_connection()
        if not conn:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM incidents WHERE id = %s;",
                        (incident_id,),
                    )
                    return cur.fetchone()
        except Exception as exc:
            logger.warning("Error fetching incident %s: %s", incident_id, exc)
            return None

    def get_incident_tool_calls(self, incident_id: str) -> list[dict[str, Any]]:
        """Query tool calls timeline for an incident."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT step, tool_name, arguments, output_truncated, started_at, duration_ms
                        FROM incident_tool_calls
                        WHERE incident_id = %s
                        ORDER BY step ASC;
                        """,
                        (incident_id,),
                    )
                    return cur.fetchall()
        except Exception as exc:
            logger.warning("Error fetching tool calls for %s: %s", incident_id, exc)
            return []

    def get_incident_audit(self, incident_id: str) -> dict[str, Any] | None:
        """Query diagnosis audit record for an incident."""
        conn = self._get_connection()
        if not conn:
            return None
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT incident_id, diagnosis, validation_results, policy_results, sandbox_results, final_action, pr_url_or_preview, pr_url_or_preview AS pr_url, email_status, model, prompt_tokens, completion_tokens, created_at
                        FROM incident_diagnosis_audit
                        WHERE incident_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1;
                        """,
                        (incident_id,),
                    )
                    return cur.fetchone()
        except Exception as exc:
            logger.warning("Error fetching audit for %s: %s", incident_id, exc)
            return None

    def get_dq_results(
        self, batch_date: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Query data quality checks history."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    query = "SELECT batch_date, table_name, check_name, observed_value, threshold_value, passed, details, created_at FROM dq_results"
                    params: list[Any] = []
                    if batch_date:
                        query += " WHERE batch_date = %s"
                        params.append(batch_date)
                    query += " ORDER BY batch_date DESC, created_at DESC LIMIT %s;"
                    params.append(limit)
                    cur.execute(query, tuple(params))
                    return cur.fetchall()
        except Exception as exc:
            logger.warning("Error querying dq_results: %s", exc)
            return []

    def get_run_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Query run execution history from pipeline_run_log."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT dag_run_id, task_id, batch_date, status, rows_in, rows_out, rows_quarantined, error_type, error_message, started_at, ended_at
                        FROM pipeline_run_log
                        ORDER BY started_at DESC
                        LIMIT %s;
                        """,
                        (limit,),
                    )
                    return cur.fetchall()
        except Exception as exc:
            logger.warning("Error querying pipeline_run_log: %s", exc)
            return []

    def get_resolved_by_retry_count(self) -> int:
        """Count transient errors resolved automatically on try > 1."""
        conn = self._get_connection()
        if not conn:
            return 0
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM pipeline_log
                        WHERE message LIKE '%Retry resolved transient error%'
                           OR message LIKE '%Try 2 succeeded automatically%';
                        """
                    )
                    row = cur.fetchone()
                    return int(row["cnt"]) if row and "cnt" in row else 0
        except Exception as exc:
            logger.warning("Error counting resolved by retry: %s", exc)
            return 0

    def get_demo_config(self) -> dict[str, Any]:
        """Fetch current demo config."""
        conn = self._get_connection()
        if not conn:
            return {"failure_mode": "none", "target_batch_date": str(date.today())}
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT failure_mode, target_batch_date FROM demo_config WHERE id = 1;"
                    )
                    row = cur.fetchone()
                    if row:
                        return {
                            "failure_mode": row["failure_mode"],
                            "target_batch_date": str(row["target_batch_date"]),
                        }
                    return {"failure_mode": "none", "target_batch_date": str(date.today())}
        except Exception as exc:
            logger.warning("Error querying demo_config: %s", exc)
            return {"failure_mode": "none", "target_batch_date": str(date.today())}

    def set_demo_config(self, failure_mode: str, target_batch_date: str) -> bool:
        """Update demo_config table."""
        conn = self._get_connection()
        if not conn:
            return False
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO demo_config (id, failure_mode, target_batch_date, updated_at)
                        VALUES (1, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (id) DO UPDATE
                        SET failure_mode = EXCLUDED.failure_mode,
                            target_batch_date = EXCLUDED.target_batch_date,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        (failure_mode, target_batch_date),
                    )
            return True
        except Exception as exc:
            logger.warning("Error updating demo_config: %s", exc)
            return False
