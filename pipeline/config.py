"""Pipeline configuration loader."""

import os
from pathlib import Path
from typing import Any

import yaml


def get_project_root() -> Path:
    """Return the repository root directory."""
    return Path(__file__).resolve().parent.parent


def load_yaml_config(relative_path: str) -> dict[str, Any]:
    """Load a YAML configuration file relative to the project root.

    Args:
        relative_path: Path string relative to root, e.g. 'config/silver_rules.yml'.

    Returns:
        Parsed configuration dictionary.
    """
    path = get_project_root() / relative_path
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


DEMO_MODE: bool = os.getenv("DEMO_MODE", "true").lower() == "true"
PR_MODE: str = os.getenv("PR_MODE", "dry_run").lower()
AGENT_ENABLED: bool = os.getenv("AGENT_ENABLED", "true").lower() == "true"
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o")
