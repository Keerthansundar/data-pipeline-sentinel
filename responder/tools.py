"""Agent read-only inspection tools."""

import json
from typing import Any

from pipeline.config import get_project_root, load_yaml_config
from pipeline.logging_config import get_logger
from responder.policy import compute_source_ambiguity
from responder.redact import redact_data, redact_text
from responder.repository import OpsRepository

logger = get_logger(__name__)
_repo = OpsRepository()


def set_repository(repo: OpsRepository) -> None:
    """Allow injecting a mocked repository for testing."""
    global _repo
    _repo = repo


def fetch_task_error_log(incident_id: str) -> dict[str, Any]:
    """Fetch error message, stack trace, and prior 50 log lines from pipeline_log.

    Args:
        incident_id: Incident ID string.

    Returns:
        Dictionary containing error details and redacted recent log lines.
    """
    raw_data = _repo.get_task_error_log(incident_id)
    if not raw_data.get("error_message") and not raw_data.get("recent_logs"):
        return {
            "incident_id": incident_id,
            "status": "no data available for this incident",
            "error_message": "",
            "traceback": "",
            "recent_logs": [],
        }

    return redact_data(raw_data)


def fetch_run_history(task_id: str) -> list[dict[str, Any]]:
    """Fetch past run durations and statuses for a task.

    Args:
        task_id: Airflow task identifier.

    Returns:
        List of past execution summaries.
    """
    history = _repo.get_run_history(task_id, limit=10)
    if not history:
        return [{"message": f"no data available for task {task_id}"}]
    return redact_data(history)


def inspect_column_profile(batch_date: str) -> dict[str, Any]:
    """Fetch column profiling data for the batch versus previous good batch.

    Args:
        batch_date: Target batch date YYYY-MM-DD.

    Returns:
        Profile metrics and comparison with prior good batch.
    """
    current_profiles = _repo.get_column_profiles(batch_date)
    if not current_profiles:
        return {"message": f"no column profile data available for batch {batch_date}"}

    prev_date = _repo.get_last_good_batch_date(batch_date)
    prev_profiles_by_col = {}
    if prev_date:
        for p in _repo.get_column_profiles(prev_date):
            prev_profiles_by_col[p["column_name"]] = p

    comparison: dict[str, Any] = {
        "batch_date": batch_date,
        "previous_good_batch_date": prev_date or "none",
        "columns": {},
    }

    for p in current_profiles:
        c_name = p["column_name"]
        patterns = (
            json.loads(p["patterns"])
            if isinstance(p.get("patterns"), str)
            else (p.get("patterns") or {})
        )
        prev_null_rate = prev_profiles_by_col.get(c_name, {}).get("null_rate", 0.0)
        ambiguous_share = p.get("ambiguous_day_month_share", 0.0)
        if c_name == "order_date":
            source_ambiguity = compute_source_ambiguity(batch_date)
            if source_ambiguity is not None:
                ambiguous_share = source_ambiguity

        comparison["columns"][c_name] = {
            "total_rows": p.get("total_rows", 0),
            "null_count": p.get("null_count", 0),
            "null_rate": p.get("null_rate", 0.0),
            "previous_null_rate": prev_null_rate,
            "distinct_count": p.get("distinct_count", 0),
            "ambiguous_day_month_share": ambiguous_share,
            "patterns": patterns,
        }

    return redact_data(comparison)


def diff_schema_vs_last_good_batch(batch_date: str) -> dict[str, Any]:
    """Diff schema against the previous successful batch.

    Args:
        batch_date: Target batch date YYYY-MM-DD.

    Returns:
        Dictionary of added, removed, and type-changed columns.
    """
    current_schema = _repo.get_schema_snapshot(batch_date, "bronze.orders")
    if not current_schema:
        return {"message": f"no schema snapshot available for batch {batch_date}"}

    prev_date = _repo.get_last_good_batch_date(batch_date)
    if not prev_date:
        return {
            "batch_date": batch_date,
            "previous_good_batch": None,
            "current_columns": list(current_schema.keys()),
            "message": "no previous good batch to compare against",
        }

    prev_schema = _repo.get_schema_snapshot(prev_date, "bronze.orders") or {}

    curr_cols = set(current_schema.keys())
    prev_cols = set(prev_schema.keys())

    added = list(curr_cols - prev_cols)
    removed = list(prev_cols - curr_cols)
    type_changes = {}
    for c in curr_cols.intersection(prev_cols):
        if str(current_schema[c]) != str(prev_schema[c]):
            type_changes[c] = {
                "previous_type": str(prev_schema[c]),
                "current_type": str(current_schema[c]),
            }

    return {
        "batch_date": batch_date,
        "previous_good_batch": prev_date,
        "added_columns": sorted(added),
        "removed_columns": sorted(removed),
        "type_changes": type_changes,
        "all_current_columns": sorted(list(curr_cols)),
    }


def fetch_data_quality_results(batch_date: str) -> list[dict[str, Any]]:
    """Fetch DQ check results for the batch.

    Args:
        batch_date: Target batch date YYYY-MM-DD.

    Returns:
        List of DQ check results.
    """
    results = _repo.get_dq_results(batch_date)
    if not results:
        return [{"message": f"no data quality results recorded for batch {batch_date}"}]
    return redact_data(results)


def trace_downstream_impact(task_id: str) -> dict[str, Any]:
    """Trace downstream tables and dashboard pages affected by a task failure.

    Args:
        task_id: Airflow task identifier.

    Returns:
        Dictionary mapping task to downstream tables and dashboards.
    """
    lineage_path = "config/lineage.yml"
    try:
        lineage_cfg = load_yaml_config(lineage_path)
        task_lineage = lineage_cfg.get("lineage", {}).get(task_id, {})
        return {
            "task_id": task_id,
            "downstream_tables": task_lineage.get("downstream_tables", []),
            "dashboard_pages": task_lineage.get("dashboard_pages", []),
        }
    except Exception as exc:
        logger.warning("Error reading lineage config: %s", exc)
        return {
            "task_id": task_id,
            "downstream_tables": [],
            "dashboard_pages": [],
            "message": f"lineage lookup failed: {exc}",
        }


def read_silver_rules() -> str:
    """Read current contents of config/silver_rules.yml (the only readable file).

    Returns:
        Redacted string content of silver_rules.yml.
    """
    root = get_project_root()
    rules_path = root / "config" / "silver_rules.yml"
    if not rules_path.exists():
        return "no data available: config/silver_rules.yml not found"

    try:
        with open(rules_path, encoding="utf-8") as f:
            content = f.read()
        return redact_text(content)
    except Exception as exc:
        return f"error reading silver rules: {exc}"


def submit_incident_diagnosis(diagnosis: dict[str, Any]) -> dict[str, Any]:
    """Submit final structured diagnosis and end agent loop.

    Args:
        diagnosis: Structured diagnosis dictionary.

    Returns:
        Submitted diagnosis dictionary.
    """
    return diagnosis


TOOL_DISPATCH = {
    "fetch_task_error_log": fetch_task_error_log,
    "fetch_run_history": fetch_run_history,
    "inspect_column_profile": inspect_column_profile,
    "diff_schema_vs_last_good_batch": diff_schema_vs_last_good_batch,
    "fetch_data_quality_results": fetch_data_quality_results,
    "trace_downstream_impact": trace_downstream_impact,
    "read_silver_rules": read_silver_rules,
    "submit_incident_diagnosis": submit_incident_diagnosis,
}
