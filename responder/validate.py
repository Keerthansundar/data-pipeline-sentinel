"""Diagnosis validation including evidence citation checks."""

from typing import Any

from pydantic import ValidationError

from responder.schemas import EvidenceItem, IncidentDiagnosis, ProposedAction


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


class CitationResult(tuple):
    """Result of citation validation as (passed, error) with dropped_items attribute."""

    passed: bool
    error: str | None
    dropped_items: list[dict[str, str]]

    def __new__(
        cls,
        passed: bool,
        error: str | None,
        dropped_items: list[dict[str, str]] | None = None,
    ):
        instance = super().__new__(cls, (passed, error))
        instance.passed = passed
        instance.error = error
        instance.dropped_items = dropped_items or []
        return instance


def validate_evidence_citations(
    diagnosis: IncidentDiagnosis, tool_outputs: dict[str, str]
) -> CitationResult:
    """Verify that every cited quote appears verbatim in the cited tool output.

    Removes unverifiable evidence items from diagnosis.evidence.
    Fails strictly if an evidence item's tool was never called, or if a rules_patch
    has no verified quote remaining from fetch_task_error_log.
    Escalate-without-patch diagnoses remain valid even if all evidence was dropped.

    Args:
        diagnosis: Validated IncidentDiagnosis object.
        tool_outputs: Mapping of tool names (or tool_call_ids) to string output returned by tool.

    Returns:
        CitationResult tuple (passed, error) with .dropped_items attribute.
    """

    def _collapse(text: str) -> str:
        return " ".join(text.split())

    dropped_items: list[dict[str, str]] = []
    verified_evidence: list[EvidenceItem] = []

    for idx, item in enumerate(diagnosis.evidence or []):
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
            return CitationResult(
                False,
                f"Evidence item {idx} references tool '{clean_tool_name}' which was not called in this run",
                dropped_items,
            )

        if not item.quote or len(item.quote.strip()) == 0:
            dropped_items.append({"tool_name": clean_tool_name, "quote": ""})
            continue

        quote_collapsed = _collapse(item.quote)
        output_collapsed = _collapse(output_str)

        if quote_collapsed in output_collapsed:
            verified_evidence.append(item)
        else:
            dropped_items.append({"tool_name": clean_tool_name, "quote": item.quote[:80]})

    diagnosis.evidence = verified_evidence

    is_rules_patch = (
        diagnosis.proposed_action == ProposedAction.RULES_PATCH
        or diagnosis.proposed_action == "rules_patch"
    )
    if is_rules_patch:
        has_verified_error_log = any(
            (ev.tool_name or "").removeprefix("functions.") == "fetch_task_error_log"
            for ev in verified_evidence
        )
        if not has_verified_error_log:
            msg = "Rules patch requires a verified quote from fetch_task_error_log"
            if dropped_items:
                msg += ": quote does not appear verbatim in tool output"
            return CitationResult(False, msg, dropped_items)

    return CitationResult(True, None, dropped_items)
