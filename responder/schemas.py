"""Pydantic schemas and constants for the incident responder."""

from enum import StrEnum
from typing import Any

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
    evidence: list[EvidenceItem] = Field(default_factory=list)
    proposed_action: ProposedAction
    patch: RulesPatch | None = None
    escalation_reason: str | None = None
    recommended_next_steps: str


OPENAI_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "fetch_task_error_log",
            "description": "Fetch error message, stack trace, and prior 50 log lines from pipeline_log.",
            "parameters": {
                "type": "object",
                "properties": {
                    "incident_id": {
                        "type": "string",
                        "description": "Unique identifier of the incident (e.g. INC-2026-10-01-...)",
                    }
                },
                "required": ["incident_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_run_history",
            "description": "Fetch past run statuses and durations for a task.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Airflow task ID (e.g. silver_transform, bronze_ingest)",
                    }
                },
                "required": ["task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "inspect_column_profile",
            "description": "Fetch column profiling metrics (null rates, patterns, ambiguous_day_month_share) for the batch.",
            "parameters": {
                "type": "object",
                "properties": {
                    "batch_date": {
                        "type": "string",
                        "description": "Target batch date in YYYY-MM-DD format",
                    }
                },
                "required": ["batch_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "diff_schema_vs_last_good_batch",
            "description": "Diff the batch schema snapshot against the most recent successful batch.",
            "parameters": {
                "type": "object",
                "properties": {
                    "batch_date": {
                        "type": "string",
                        "description": "Target batch date in YYYY-MM-DD format",
                    }
                },
                "required": ["batch_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_data_quality_results",
            "description": "Fetch data quality check results for the batch.",
            "parameters": {
                "type": "object",
                "properties": {
                    "batch_date": {
                        "type": "string",
                        "description": "Target batch date in YYYY-MM-DD format",
                    }
                },
                "required": ["batch_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trace_downstream_impact",
            "description": "Trace downstream Delta tables and dashboard pages affected by a task failure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Airflow task ID that failed",
                    }
                },
                "required": ["task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_silver_rules",
            "description": "Read current contents of config/silver_rules.yml (the only readable rules file).",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "submit_incident_diagnosis",
            "description": "Submit final structured incident diagnosis and end the investigation loop.",
            "parameters": {
                "type": "object",
                "properties": {
                    "root_cause_category": {
                        "type": "string",
                        "enum": [
                            "transient",
                            "late_data",
                            "format_drift",
                            "schema_drift",
                            "data_quality",
                            "infrastructure",
                            "ambiguous",
                            "unknown",
                        ],
                    },
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "summary": {"type": "string"},
                    "impact": {"type": "string"},
                    "evidence": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "tool_name": {"type": "string"},
                                "tool_call_id": {"type": "string"},
                                "quote": {"type": "string", "maxLength": 200},
                            },
                            "required": ["tool_name", "tool_call_id", "quote"],
                        },
                    },
                    "proposed_action": {"type": "string", "enum": ["rules_patch", "escalate"]},
                    "patch": {
                        "type": "object",
                        "properties": {
                            "file": {"type": "string"},
                            "operation": {"type": "string"},
                            "key_path": {"type": "string"},
                            "value": {
                                "type": "string",
                                "description": (
                                    "The exact replacement or appended format string as a plain string, "
                                    "such as '%d/%m/%Y'. Never use an object or dict. "
                                    "The proposed format must be taken from sample_bad_values in the error log "
                                    "(e.g. sample '21/10/2026' means '%d/%m/%Y')."
                                ),
                            },
                        },
                        "required": ["file", "operation", "key_path", "value"],
                    },
                    "escalation_reason": {"type": "string"},
                    "recommended_next_steps": {"type": "string"},
                },
                "required": [
                    "root_cause_category",
                    "confidence",
                    "summary",
                    "impact",
                    "proposed_action",
                    "recommended_next_steps",
                ],
            },
        },
    },
]
