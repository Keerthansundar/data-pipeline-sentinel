"""Script to safely remove a stray or test batch date from Delta tables and Ops Postgres.

Usage:
    python tests/cleanup_batch.py <batch_date>
Example:
    python tests/cleanup_batch.py 2026-11-01
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any

from deltalake import DeltaTable

try:
    import psycopg
except ImportError:
    try:
        import psycopg2 as psycopg  # type: ignore[no-redef]
    except ImportError:
        psycopg = None  # type: ignore[assignment]

DELTA_TABLE_PATHS = [
    "data/bronze/orders",
    "data/bronze/customers",
    "data/silver/orders",
    "data/silver/customers",
    "data/quarantine/orders",
    "data/gold/daily_revenue",
    "data/gold/revenue_by_channel",
    "data/gold/customer_summary",
]

OPS_TABLES_TO_CLEAN = [
    (
        "incident_diagnosis_audit",
        "DELETE FROM incident_diagnosis_audit WHERE incident_id IN (SELECT id FROM incidents WHERE batch_date = %s);",
    ),
    (
        "incident_tool_calls",
        "DELETE FROM incident_tool_calls WHERE incident_id IN (SELECT id FROM incidents WHERE batch_date = %s);",
    ),
    ("incidents", "DELETE FROM incidents WHERE batch_date = %s;"),
    ("dq_results", "DELETE FROM dq_results WHERE batch_date = %s;"),
    ("column_profile", "DELETE FROM column_profile WHERE batch_date = %s;"),
    ("schema_snapshot", "DELETE FROM schema_snapshot WHERE batch_date = %s;"),
    ("pipeline_run_log", "DELETE FROM pipeline_run_log WHERE batch_date = %s;"),
    ("pipeline_log", "DELETE FROM pipeline_log WHERE batch_date = %s;"),
]


def validate_batch_date(batch_date: str) -> bool:
    """Validate that batch_date is YYYY-MM-DD."""
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", batch_date))


def delete_from_delta_tables(batch_date: str, root_dir: Path | None = None) -> dict[str, str]:
    """Delete batch date records from all Delta tables via Delta DELETE."""
    if not validate_batch_date(batch_date):
        raise ValueError(f"Invalid batch date format: '{batch_date}'. Expected YYYY-MM-DD.")

    root = root_dir or Path.cwd()
    results: dict[str, str] = {}
    for rel_path in DELTA_TABLE_PATHS:
        table_path = root / rel_path
        if table_path.exists() and (table_path / "_delta_log").exists():
            try:
                dt = DeltaTable(str(table_path))
                predicate = f"_batch_date = '{batch_date}'"
                dt.delete(predicate)
                results[rel_path] = "deleted"
            except Exception as exc:
                results[rel_path] = f"error: {exc}"
        else:
            results[rel_path] = "not found"
    return results


def delete_from_ops_db(batch_date: str, conn: Any = None) -> dict[str, int]:
    """Delete batch date records from Postgres Ops DB tables."""
    if not validate_batch_date(batch_date):
        raise ValueError(f"Invalid batch date format: '{batch_date}'. Expected YYYY-MM-DD.")

    close_conn = False
    if conn is None:
        if psycopg is None:
            raise RuntimeError("Neither psycopg nor psycopg2 is installed.")
        db_host = os.getenv("OPS_DB_HOST", "localhost")
        db_port = int(os.getenv("OPS_DB_PORT", "5434"))
        db_name = os.getenv("OPS_DB_NAME", "ops")
        db_user = os.getenv("OPS_DB_USER", "ops_user")
        db_pass = os.getenv("OPS_DB_PASSWORD", "ops_password")
        conn = psycopg.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass,
        )
        close_conn = True

    deleted_counts: dict[str, int] = {}
    try:
        with conn:
            with conn.cursor() as cur:
                for table_name, sql in OPS_TABLES_TO_CLEAN:
                    cur.execute(sql, (batch_date,))
                    deleted_counts[table_name] = cur.rowcount
    finally:
        if close_conn:
            conn.close()

    return deleted_counts


def run_cleanup(batch_date: str, root_dir: Path | None = None, conn: Any = None) -> None:
    """Execute complete cleanup for the specified batch date."""
    print(f"=== Starting batch cleanup for date: {batch_date} ===")
    delta_res = delete_from_delta_tables(batch_date, root_dir=root_dir)
    print("\nDelta Tables:")
    for path, status in delta_res.items():
        print(f"  - {path}: {status}")

    try:
        ops_res = delete_from_ops_db(batch_date, conn=conn)
        print("\nOps Database Tables:")
        for table, count in ops_res.items():
            print(f"  - {table}: {count} row(s) deleted")
    except Exception as exc:
        print(f"\nOps DB cleanup warning: {exc}")

    print(f"\n=== Completed cleanup for batch date: {batch_date} ===")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tests/cleanup_batch.py <batch_date>")
        sys.exit(1)
    target_date = sys.argv[1].strip()
    if not validate_batch_date(target_date):
        print(f"Error: Invalid batch date format '{target_date}'. Expected YYYY-MM-DD.")
        sys.exit(1)
    run_cleanup(target_date)
