"""Diagnosis validation including evidence citation checks."""

from typing import Any

from pydantic import ValidationError

from responder.schemas import IncidentDiagnosis


def validate_diagnosis_schema(
    raw_diagnosis: dict[str, Any],
) -> tuple[bool, IncidentDiagnosis | None, str | None]:
    """Validate dictionary against IncidentDiagnosis Pydantic schema.

    Args:
        raw_diagnosis: Raw diagnosis dictionary from LLM or tool call.

    Returns:
        Tuple of (is_valid, parsed_diagnosis_object, error_message).
    """
    try:
        if not isinstance(raw_diagnosis, dict):
            return False, None, f"raw_diagnosis must be a dict, got {type(raw_diagnosis).__name__}"

        # Truncate each quote to 200 chars before schema validation
        evidence = raw_diagnosis.get("evidence")
        if isinstance(evidence, list):
            for item in evidence:
                if isinstance(item, dict) and "quote" in item:
                    quote = item["quote"]
                    if isinstance(quote, str) and len(quote) > 200:
                        item["quote"] = quote[:200]

        # Reject dict patch.value cleanly
        patch_info = raw_diagnosis.get("patch")
        if isinstance(patch_info, dict):
            val = patch_info.get("value")
            if isinstance(val, dict):
                return (
                    False,
                    None,
                    "patch.value must be a plain string (such as '%d/%m/%Y'), not a dict",
                )

        diagnosis = IncidentDiagnosis.model_validate(raw_diagnosis)
        return True, diagnosis, None
    except ValidationError as exc:
        return False, None, str(exc)
    except Exception as exc:
        return False, None, f"Unexpected validation error: {exc}"


def validate_evidence_citations(
    diagnosis: IncidentDiagnosis, tool_outputs: dict[str, str]
) -> tuple[bool, str | None]:
    """Verify that every cited quote appears verbatim in the cited tool output.

    Args:
        diagnosis: Validated IncidentDiagnosis object.
        tool_outputs: Mapping of tool names (or tool_call_ids) to string output returned by tool.

    Returns:
        Tuple of (all_valid, failure_reason).
    """
    if not diagnosis.evidence:
        # If proposed action is rules_patch, evidence is strictly required
        if diagnosis.proposed_action == "rules_patch":
            return False, "Rules patch proposed without supporting evidence citations"
        return True, None

    def _collapse(text: str) -> str:
        return " ".join(text.split())

    for idx, item in enumerate(diagnosis.evidence):
        if not item.quote or len(item.quote.strip()) == 0:
            return False, f"Evidence item {idx} has an empty quote"

        raw_tool_name = item.tool_name or ""
        clean_tool_name = raw_tool_name.removeprefix("functions.")

        output_str: str | None = None
        if clean_tool_name in tool_outputs:
            output_str = tool_outputs[clean_tool_name]
        elif raw_tool_name in tool_outputs:
            output_str = tool_outputs[raw_tool_name]
        elif item.tool_call_id and item.tool_call_id in tool_outputs:
            output_str = tool_outputs[item.tool_call_id]

        if output_str is None:
            return (
                False,
                f"Evidence item {idx} references tool '{clean_tool_name}' which was not called in this run",
            )

        quote_collapsed = _collapse(item.quote)
        output_collapsed = _collapse(output_str)

        if quote_collapsed not in output_collapsed:
            return (
                False,
                f"Evidence citation mismatch in item {idx}: quote '{item.quote[:50]}...' "
                f"does not appear verbatim in tool '{clean_tool_name}' output",
            )

    return True, None
