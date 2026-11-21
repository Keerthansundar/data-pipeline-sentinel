"""Pydantic schemas and constants for the incident responder."""

from enum import StrEnum

from pydantic import BaseModel, Field

TOOL_NAMES = [
    "fetch_task_error_log",
    "fetch_run_history",
    "inspect_column_profile",
    "diff_schema_vs_last_good_batch",
    "fetch_data_quality_results",
    "trace_downstream_impact",
    "read_silver_rules",
    "submit_incident_diagnosis",
]


class RootCauseCategory(StrEnum):
    """Categorization of pipeline incident root cause."""

    TRANSIENT = "transient"
    LATE_DATA = "late_data"
    FORMAT_DRIFT = "format_drift"
    SCHEMA_DRIFT = "schema_drift"
    DATA_QUALITY = "data_quality"
    INFRASTRUCTURE = "infrastructure"
    AMBIGUOUS = "ambiguous"
    UNKNOWN = "unknown"


class ProposedAction(StrEnum):
    """Action proposed by the diagnosis agent."""

    RULES_PATCH = "rules_patch"
    ESCALATE = "escalate"


class EvidenceItem(BaseModel):
    """Evidence item linking a verbatim quote to a specific tool call."""

    tool_name: str
    tool_call_id: str
    quote: str = Field(..., max_length=200)


class RulesPatch(BaseModel):
    """Structured patch descriptor for config/silver_rules.yml."""

    file: str = "config/silver_rules.yml"
    operation: str = "append_unique"
    key_path: str
    value: str


class IncidentDiagnosis(BaseModel):
    """Structured incident diagnosis output schema."""

    root_cause_category: RootCauseCategory
    confidence: float = Field(..., ge=0.0, le=1.0)
    summary: str
    impact: str
    evidence: list[EvidenceItem]
    proposed_action: ProposedAction
    patch: RulesPatch | None = None
    escalation_reason: str | None = None
    recommended_next_steps: str
