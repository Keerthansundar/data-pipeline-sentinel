"""Diagnosis validation including evidence citation checks."""

from responder.schemas import IncidentDiagnosis


def validate_evidence_citations(diagnosis: IncidentDiagnosis, tool_outputs: dict[str, str]) -> bool:
    """Verify that every cited quote appears verbatim in the tool output."""
    for item in diagnosis.evidence:
        output = tool_outputs.get(item.tool_call_id, "")
        if item.quote not in output:
            return False
    return True
