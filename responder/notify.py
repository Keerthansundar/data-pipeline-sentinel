"""Email notification dispatch via SMTP (Mailpit) with clean HTML styling."""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from pipeline.logging_config import get_logger
from responder.redact import redact_text

logger = get_logger(__name__)


def generate_incident_email_html(
    incident_id: str,
    diagnosis: dict[str, Any],
    pr_url_or_path: str | None = None,
    sandbox_results: dict[str, Any] | None = None,
    is_escalation: bool = False,
) -> str:
    """Generate structured HTML email content per Section 7.7.

    Sections: Evidence, Reasoning, Decision, Recommended action, plus impact, PR link, incident ID.

    Args:
        incident_id: Target incident ID.
        diagnosis: Structured diagnosis dictionary.
        pr_url_or_path: PR URL or local preview file path.
        sandbox_results: Sandbox verification summary.
        is_escalation: True if notification is an escalation.

    Returns:
        Redacted HTML email string.
    """
    summary = redact_text(diagnosis.get("summary", "No summary provided."))
    category = diagnosis.get("root_cause_category", "unknown")
    confidence = diagnosis.get("confidence", 0.0)
    impact = redact_text(diagnosis.get("impact", "Data pipeline execution halted."))
    rec_steps = redact_text(
        diagnosis.get("recommended_next_steps", "Investigate logs and resolve failure.")
    )
    escalation_reason = redact_text(diagnosis.get("escalation_reason") or "")
    evidence = diagnosis.get("evidence", [])

    status_color = "#d9534f" if is_escalation else "#28a745"
    status_title = (
        "ACTION REQUIRED: Incident Escalation" if is_escalation else "AUTOFIX: Pull Request Opened"
    )

    evidence_html_items = []
    for ev in evidence:
        q = redact_text(ev.get("quote", ""))
        t_name = ev.get("tool_name", "")
        evidence_html_items.append(
            f"<li><strong>[{t_name}]:</strong> <em>&ldquo;{q}&rdquo;</em></li>"
        )
    evidence_html = (
        f"<ul>{''.join(evidence_html_items)}</ul>"
        if evidence_html_items
        else "<p><em>No direct quotes cited.</em></p>"
    )

    decision_text = (
        f"ESCALATION: {escalation_reason}"
        if is_escalation
        else "RULES PATCH PROPOSED: Validated against policy and verified in isolated sandbox."
    )

    pr_html = ""
    if pr_url_or_path:
        pr_label = (
            "GitHub Pull Request" if pr_url_or_path.startswith("http") else "Local PR Preview"
        )
        pr_html = (
            f"<p><strong>{pr_label}:</strong> <a href='{pr_url_or_path}'>{pr_url_or_path}</a></p>"
        )

    sandbox_html = ""
    if sandbox_results and sandbox_results.get("skipped"):
        sandbox_html = """
        <div style="background-color: #f8f9fa; border-left: 4px solid #17a2b8; padding: 10px 15px; margin: 15px 0;">
            <strong>Sandbox:</strong> not run (no patch proposed)
        </div>
        """
    elif sandbox_results:
        sb_status = "PASSED" if sandbox_results.get("passed") else "FAILED"
        sandbox_html = f"""
        <div style="background-color: #f8f9fa; border-left: 4px solid #17a2b8; padding: 10px 15px; margin: 15px 0;">
            <strong>Sandbox Verification:</strong> {sb_status}<br/>
            Rows In: {sandbox_results.get("rows_in", "N/A")} |
            Rows Out: {sandbox_results.get("rows_out", "N/A")} |
            Quarantined: {sandbox_results.get("rows_quarantined", "N/A")}
        </div>
        """

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; }}
  .container {{ max-width: 650px; margin: 0 auto; padding: 20px; border: 1px solid #e1e4e8; border-radius: 6px; }}
  .header {{ border-bottom: 2px solid {status_color}; padding-bottom: 10px; margin-bottom: 20px; }}
  .header h2 {{ margin: 0; color: {status_color}; }}
  .section {{ margin-bottom: 20px; }}
  .section-title {{ font-size: 16px; font-weight: 600; text-transform: uppercase; color: #586069; border-bottom: 1px solid #eaecef; padding-bottom: 4px; margin-bottom: 8px; }}
  .badge {{ display: inline-block; padding: 2px 6px; font-size: 12px; font-weight: 600; border-radius: 3px; background-color: #e1e4e8; color: #24292e; }}
  .footer {{ font-size: 12px; color: #6a737d; border-top: 1px solid #eaecef; padding-top: 10px; margin-top: 20px; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h2>{status_title}</h2>
    <span class="badge">Incident: {incident_id}</span>
    <span class="badge">Category: {category}</span>
    <span class="badge">Confidence: {confidence:.2f}</span>
  </div>

  <div class="section">
    <div class="section-title">Evidence</div>
    {evidence_html}
  </div>

  <div class="section">
    <div class="section-title">Reasoning</div>
    <p>{summary}</p>
    <p><strong>Impact:</strong> {impact}</p>
  </div>

  <div class="section">
    <div class="section-title">Decision</div>
    <p><strong>{decision_text}</strong></p>
    {sandbox_html}
    {pr_html}
  </div>

  <div class="section">
    <div class="section-title">Recommended Action</div>
    <p>{rec_steps}</p>
  </div>

  <div class="footer">
    Sent deterministically by PipelineSentinel Incident Responder.
  </div>
</div>
</body>
</html>
"""
    return html


def send_incident_email(
    incident_id: str,
    diagnosis: dict[str, Any],
    pr_url_or_path: str | None = None,
    sandbox_results: dict[str, Any] | None = None,
    is_escalation: bool = False,
    to_email: str | None = None,
) -> bool:
    """Send formatted incident notification email through SMTP (Mailpit).

    Args:
        incident_id: Target incident identifier.
        diagnosis: Structured diagnosis dictionary.
        pr_url_or_path: PR URL or preview path.
        sandbox_results: Sandbox verification details.
        is_escalation: True if escalating to human engineer.
        to_email: Optional override for recipient.

    Returns:
        True if email was sent successfully, False otherwise.
    """
    smtp_host = os.getenv("SMTP_HOST", "mailpit")
    smtp_port = int(os.getenv("SMTP_PORT", "1025"))
    from_email = os.getenv("ALERT_EMAIL_FROM", "pipeline-sentinel@example.com")
    dest_email = to_email or os.getenv("ALERT_EMAIL_TO", "engineer@example.com")

    if is_escalation or diagnosis.get("proposed_action") == "escalate":
        subject = f"[ESCALATION] PipelineSentinel Incident {incident_id} requires attention"
    else:
        subject = f"[AUTOFIX] Pull Request opened for Incident {incident_id}"

    html_content = generate_incident_email_html(
        incident_id=incident_id,
        diagnosis=diagnosis,
        pr_url_or_path=pr_url_or_path,
        sandbox_results=sandbox_results,
        is_escalation=is_escalation,
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = dest_email

    # Plain text alternative
    text_content = (
        f"PipelineSentinel Incident Alert: {incident_id}\n\n"
        f"Summary: {diagnosis.get('summary')}\n"
        f"Category: {diagnosis.get('root_cause_category')}\n"
        f"Action: {'ESCALATE' if is_escalation else 'RULES_PATCH'}\n"
        f"PR/Preview: {pr_url_or_path or 'N/A'}\n"
        f"Recommended Steps: {diagnosis.get('recommended_next_steps')}\n"
    )
    msg.attach(MIMEText(text_content, "plain"))
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            smtp_user = os.getenv("SMTP_USER")
            smtp_pass = os.getenv("SMTP_PASSWORD")
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info(
            "Incident email sent successfully: incident=%s, to=%s, subject=%s",
            incident_id,
            dest_email,
            subject,
        )
        return True
    except Exception as exc:
        logger.warning(
            "Failed to dispatch incident email via %s:%d: %s",
            smtp_host,
            smtp_port,
            exc,
        )
        return False
