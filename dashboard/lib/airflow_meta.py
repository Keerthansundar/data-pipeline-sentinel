"""Read-only adapter for Airflow metadata database."""

import os
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


class AirflowMetaAdapter:
    """Queries Airflow dag_run and task_instance tables using read-only role."""

    def __init__(self, db_url: str | None = None):
        if db_url:
            self.db_url = db_url
        else:
            host = os.getenv("AIRFLOW_DB_HOST", "localhost")
            port = os.getenv("AIRFLOW_DB_PORT", "5432")
            dbname = os.getenv("AIRFLOW_DB_NAME", "airflow")
            user = os.getenv("AIRFLOW_DB_USER", "airflow_ro")
            password = os.getenv("AIRFLOW_DB_PASSWORD", "airflow_ro_password")
            self.db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    def _get_connection(self) -> psycopg.Connection | None:
        try:
            return psycopg.connect(self.db_url, row_factory=dict_row, connect_timeout=3)
        except Exception as exc:
            logger.warning("Could not connect to Airflow metadata DB: %s", exc)
            return None

    def get_dags(self) -> list[str]:
        """Return distinct DAG IDs present in dag_run."""
        known_dags = ["vendor_feed_simulator", "orders_medallion_daily", "incident_responder"]
        conn = self._get_connection()
        if not conn:
            return known_dags
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT dag_id FROM dag_run ORDER BY dag_id;")
                    rows = cur.fetchall()
                    found = [r["dag_id"] for r in rows if r["dag_id"]]
                    return found if found else known_dags
        except Exception as exc:
            logger.warning("Error fetching DAG list: %s", exc)
            return known_dags

    def get_dag_runs(self, dag_id: str, limit: int = 30) -> list[dict[str, Any]]:
        """Return recent runs for a given DAG."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT run_id, state, logical_date, start_date, end_date, run_type, run_after
                        FROM dag_run
                        WHERE dag_id = %s
                        ORDER BY start_date DESC NULLS LAST
                        LIMIT %s;
                        """,
                        (dag_id, limit),
                    )
                    return cur.fetchall()
        except Exception as exc:
            logger.warning("Error fetching runs for DAG %s: %s", dag_id, exc)
            return []

    def get_latest_dag_run(self, dag_id: str) -> dict[str, Any] | None:
        """Return the most recent run for a given DAG."""
        runs = self.get_dag_runs(dag_id, limit=1)
        return runs[0] if runs else None

    def get_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        """Return task instances matching Airflow Task Instances view columns."""
        conn = self._get_connection()
        if not conn:
            return []
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT task_id, rendered_map_index, state, start_date, end_date, duration, try_number, pool, operator
                        FROM task_instance
                        WHERE dag_id = %s AND run_id = %s
                        ORDER BY start_date ASC NULLS LAST;
                        """,
                        (dag_id, run_id),
                    )
                    rows = cur.fetchall()
                    formatted = []
                    for r in rows:
                        map_idx = r.get("rendered_map_index")
                        formatted.append(
                            {
                                "Task ID": r.get("task_id"),
                                "Map Index": str(map_idx) if map_idx not in (None, "") else "-",
                                "State": r.get("state") or "queued",
                                "Start Date": (
                                    r["start_date"].strftime("%Y-%m-%d %H:%M:%S")
                                    if isinstance(r.get("start_date"), datetime)
                                    else "-"
                                ),
                                "End Date": (
                                    r["end_date"].strftime("%Y-%m-%d %H:%M:%S")
                                    if isinstance(r.get("end_date"), datetime)
                                    else "-"
                                ),
                                "Duration": (
                                    f"{r['duration']:.2f}s"
                                    if r.get("duration") is not None
                                    else "-"
                                ),
                                "Try Number": r.get("try_number", 1),
                                "Pool": r.get("pool", "default_pool"),
                                "Operator": r.get("operator", "PythonOperator"),
                            }
                        )
                    return formatted
        except Exception as exc:
            logger.warning("Error fetching task instances for %s (%s): %s", dag_id, run_id, exc)
            return []
