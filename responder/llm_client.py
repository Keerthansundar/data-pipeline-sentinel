"""Thin wrapper over OpenAI client to facilitate complete mocking in tests."""

import os


def get_openai_client():
    """Return OpenAI client instance initialized with env API key."""
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is missing")
    return OpenAI(api_key=api_key)
