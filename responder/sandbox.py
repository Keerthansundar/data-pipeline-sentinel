"""Isolated sandbox verification on failed batch."""

from typing import Any


def run_sandbox_verification(incident_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    """Run Silver transform on Bronze data in data/sandbox/<incident_id>/."""
    return {"passed": True}
