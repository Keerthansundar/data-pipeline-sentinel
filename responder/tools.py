"""Agent read-only inspection tools."""

from typing import Any


def fetch_task_error_log(incident_id: str) -> dict[str, Any]:
    """Fetch error message, stack trace, and prior 50 log lines from pipeline_log."""
    return {"error_message": "", "traceback": "", "recent_logs": []}


def fetch_run_history(task_id: str) -> list[dict[str, Any]]:
    """Fetch past run durations and statuses for the task."""
    return []


def inspect_column_profile(batch_date: str) -> dict[str, Any]:
    """Fetch column profiling data for the batch."""
    return {}


def diff_schema_vs_last_good_batch(batch_date: str) -> dict[str, Any]:
    """Diff schema against the previous successful batch."""
    return {}


def fetch_data_quality_results(batch_date: str) -> list[dict[str, Any]]:
    """Fetch failed DQ checks for the batch."""
    return []


def trace_downstream_impact(task_id: str) -> dict[str, Any]:
    """Trace downstream tables and dashboard pages affected."""
    return {}


def read_silver_rules() -> str:
    """Read current contents of config/silver_rules.yml."""
    return ""


def submit_incident_diagnosis(diagnosis: dict[str, Any]) -> dict[str, Any]:
    """Submit final structured diagnosis."""
    return diagnosis
