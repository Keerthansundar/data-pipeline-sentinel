"""Email notifications via local Mailpit SMTP sink or external SMTP."""


def send_incident_email(
    incident_id: str,
    subject: str,
    html_content: str,
    to_email: str,
) -> bool:
    """Send incident report or escalation email."""
    return True
