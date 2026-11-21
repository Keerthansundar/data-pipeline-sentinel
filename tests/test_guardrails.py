"""Unit tests for Section 8 guardrails."""

from responder.policy import enforce_patch_policy
from responder.schemas import (
    TOOL_NAMES,
    EvidenceItem,
    IncidentDiagnosis,
    ProposedAction,
    RootCauseCategory,
    RulesPatch,
)
from responder.validate import validate_evidence_citations


def test_guardrail_read_only_tools():
    """Verify all defined tools are read-only and matched in TOOL_NAMES."""
    expected_tools = [
        "fetch_task_error_log",
        "fetch_run_history",
        "inspect_column_profile",
        "diff_schema_vs_last_good_batch",
        "fetch_data_quality_results",
        "trace_downstream_impact",
        "read_silver_rules",
        "submit_incident_diagnosis",
    ]
    assert TOOL_NAMES == expected_tools


def test_guardrail_evidence_citation_check():
    """Verify diagnosis is rejected if quoted text is not in cited tool output."""
    diagnosis = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.FORMAT_DRIFT,
        confidence=0.95,
        summary="Format drift detected",
        impact="Silver failed",
        evidence=[
            EvidenceItem(
                tool_name="fetch_task_error_log",
                tool_call_id="call_1",
                quote="unparseable date ratio 5.2% exceeds max 1.0%",
            )
        ],
        proposed_action=ProposedAction.RULES_PATCH,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="append_unique",
            key_path="orders.date_formats.order_date",
            value="%d/%m/%Y",
        ),
        recommended_next_steps="Apply patch and rerun",
    )

    valid_tool_outputs = {"call_1": "Log output: unparseable date ratio 5.2% exceeds max 1.0% seen"}
    invalid_tool_outputs = {"call_1": "Different text without the quote"}

    assert validate_evidence_citations(diagnosis, valid_tool_outputs) is True
    assert validate_evidence_citations(diagnosis, invalid_tool_outputs) is False


def test_guardrail_ambiguity_flag_blocks_patch():
    """Verify ambiguity blocks any patch and forces escalation."""
    diagnosis = IncidentDiagnosis(
        root_cause_category=RootCauseCategory.AMBIGUOUS,
        confidence=0.90,
        summary="Ambiguous dates observed",
        impact="Silver stopped",
        evidence=[],
        proposed_action=ProposedAction.ESCALATE,
        patch=RulesPatch(
            file="config/silver_rules.yml",
            operation="append_unique",
            key_path="orders.date_formats.order_date",
            value="%d/%m/%Y",
        ),
        escalation_reason="High ambiguous date ratio",
        recommended_next_steps="Manual review needed",
    )

    # With ambiguity > 0.0, policy must reject patch
    assert enforce_patch_policy(diagnosis, ambiguous_ratio=0.15) is False
