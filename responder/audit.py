"""Audit persistence for incident investigations."""

from typing import Any

from pipeline.logging_config import get_logger
from responder.repository import OpsRepository

logger = get_logger(__name__)


def record_incident_audit(
    incident_id: str,
    diagnosis: dict[str, Any],
    validation_results: dict[str, Any],
    policy_results: dict[str, Any],
    sandbox_results: dict[str, Any] | None,
    final_action: str,
    pr_url: str | None = None,
    email_status: str = "PENDING",
    model: str | None = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    repo: OpsRepository | None = None,
) -> None:
    """Save full audit record to incident_diagnosis_audit table.

    Args:
        incident_id: Target incident ID.
        diagnosis: Structured diagnosis dictionary.
        validation_results: Schema and citation validation results.
        policy_results: Policy enforcement results.
        sandbox_results: Sandbox verification metrics and check statuses.
        final_action: Final action taken ('pr_opened', 'escalate', etc.).
        pr_url: URL or preview path of the pull request.
        email_status: Status of email dispatch ('SENT', 'FAILED', 'PENDING').
        model: Model name used for diagnosis.
        prompt_tokens: Prompt token count.
        completion_tokens: Completion token count.
        repo: Optional injected OpsRepository.
    """
    if repo is None:
        repo = OpsRepository()

    repo.record_audit(
        incident_id=incident_id,
        diagnosis=diagnosis,
        validation_results=validation_results,
        policy_results=policy_results,
        sandbox_results=sandbox_results,
        final_action=final_action,
        pr_url=pr_url,
        email_status=email_status,
        model=model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )
    logger.info(
        "Recorded audit for incident %s: action=%s, pr_url=%s, email_status=%s",
        incident_id,
        final_action,
        pr_url,
        email_status,
    )
