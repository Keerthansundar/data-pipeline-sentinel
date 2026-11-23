"""PII and secret redaction utilities for PipelineSentinel."""

import re
from typing import Any

# Regex patterns for scrubbing sensitive values
_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_OPENAI_KEY_PATTERN = re.compile(r"sk-[a-zA-Z0-9_-]{20,}")
_GITHUB_TOKEN_PATTERN = re.compile(r"(gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})")
_BEARER_PATTERN = re.compile(r"Bearer\s+[a-zA-Z0-9_\-\.=:_+/]+", re.IGNORECASE)
_PASSWORD_PATTERN = re.compile(
    r"\b(password|pwd|secret)\s*[:=]\s*(['\"][^'\"]*['\"]|[^\s,;]+)",
    re.IGNORECASE,
)


def redact_text(text: str | None) -> str:
    """Scrub emails, API keys, tokens, and credentials from a text string.

    Args:
        text: The raw string to sanitize.

    Returns:
        The sanitized string with sensitive patterns masked.
    """
    if not text:
        return ""

    sanitized = _EMAIL_PATTERN.sub("[REDACTED_EMAIL]", text)
    sanitized = _OPENAI_KEY_PATTERN.sub("[REDACTED_OPENAI_KEY]", sanitized)
    sanitized = _GITHUB_TOKEN_PATTERN.sub("[REDACTED_GITHUB_TOKEN]", sanitized)
    sanitized = _BEARER_PATTERN.sub("Bearer [REDACTED_TOKEN]", sanitized)
    sanitized = _PASSWORD_PATTERN.sub(r"\1=[REDACTED_SECRET]", sanitized)

    return sanitized


def redact_data(data: Any) -> Any:
    """Recursively scrub sensitive information from arbitrary nested structures.

    Args:
        data: Arbitrary data structure (dict, list, str, etc.)

    Returns:
        A deep sanitized copy of the input data structure.
    """
    if isinstance(data, str):
        return redact_text(data)
    if isinstance(data, dict):
        return {k: redact_data(v) for k, v in data.items()}
    if isinstance(data, list):
        return [redact_data(v) for v in data]
    return data
