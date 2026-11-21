"""OpenAI tool-calling agent loop for incident diagnosis."""

from responder.schemas import IncidentDiagnosis


def diagnose_incident(incident_id: str) -> IncidentDiagnosis:
    """Run diagnosis agent on an open incident."""
    pass
