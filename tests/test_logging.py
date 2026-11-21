"""Tests for Section 5A structured logging and redaction standard."""

import logging

from pipeline.logging_config import get_logger
from responder.redact import redact_text


def test_redact_text_removes_sensitive_information():
    """Verify redact_text scrubs emails, api keys, and bearer tokens."""
    raw = (
        "User test.user@example.com logged in with key sk-1234567890abcdef1234567890 "
        "and token ghp_1234567890abcdef1234567890abcdef1234. Bearer eyJhbGciOi"
    )
    redacted = redact_text(raw)
    assert "test.user@example.com" not in redacted
    assert "[REDACTED_EMAIL]" in redacted
    assert "sk-1234567890abcdef1234567890" not in redacted
    assert "[REDACTED_OPENAI_KEY]" in redacted
    assert "ghp_1234567890abcdef1234567890abcdef1234" not in redacted
    assert "[REDACTED_GITHUB_TOKEN]" in redacted
    assert "Bearer [REDACTED_TOKEN]" in redacted


def test_get_logger_context_enrichment(caplog):
    """Verify logger formats context fields into records."""
    logger = get_logger(
        "test_module",
        dag_id="test_dag",
        task_id="test_task",
        run_id="run_123",
        batch_date="2026-10-01",
        try_number="1",
    )
    with caplog.at_level(logging.INFO):
        logger.info("Test context message")

    assert len(caplog.records) >= 1
    rec = caplog.records[-1]
    assert rec.dag_id == "test_dag"
    assert rec.task_id == "test_task"
    assert rec.batch_date == "2026-10-01"
