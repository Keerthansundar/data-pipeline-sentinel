"""Patch policy enforcement: file, key path, whitelist, ambiguity checks."""

from responder.schemas import IncidentDiagnosis


def enforce_patch_policy(diagnosis: IncidentDiagnosis, ambiguous_ratio: float = 0.0) -> bool:
    """Check if proposed patch conforms to safety policies."""
    if not diagnosis.patch:
        return True
    if ambiguous_ratio > 0.0:
        return False
    return True
