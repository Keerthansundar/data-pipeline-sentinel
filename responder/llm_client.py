"""Thin wrapper over OpenAI client to facilitate complete mocking in tests."""

import os
from typing import Any

from pipeline.logging_config import get_logger

logger = get_logger(__name__)


def get_openai_model() -> str:
    """Retrieve OpenAI model from environment. Raises if unset."""
    model = os.getenv("OPENAI_MODEL")
    if not model:
        raise ValueError("OPENAI_MODEL environment variable is missing")
    return model


def get_openai_client() -> Any:
    """Return OpenAI client instance initialized with env API key.

    Returns:
        OpenAI client instance.

    Raises:
        ValueError: If OPENAI_API_KEY is not set.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is missing")

    from openai import OpenAI

    return OpenAI(api_key=api_key)


def execute_completion(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    client: Any | None = None,
    model: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 1500,
) -> Any:
    """Execute a chat completion request via OpenAI API.

    Args:
        messages: Chat conversation messages.
        tools: Optional list of OpenAI tool definitions.
        client: Optional injected OpenAI client (for testing).
        model: Model name. If None, loaded from env OPENAI_MODEL.
        temperature: Sampling temperature.
        max_tokens: Output token limit.

    Returns:
        ChatCompletion response object.
    """
    if client is None:
        client = get_openai_client()

    if model is None:
        model = get_openai_model()

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if tools:
        kwargs["tools"] = tools

    return client.chat.completions.create(**kwargs)
