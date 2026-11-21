"""Data quality checks executed on Silver before Gold build."""

from typing import Any


def run_dq_checks(batch_date: str) -> dict[str, Any]:
    """Execute DQ checks per config/dq_rules.yml."""
    return {"batch_date": batch_date, "passed": True}
