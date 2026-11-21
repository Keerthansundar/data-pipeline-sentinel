"""Rules patcher using ruamel.yaml for minimal round-trip diffs."""

from pathlib import Path
from typing import Any


def apply_patch_to_file(file_path: Path, patch: dict[str, Any]) -> str:
    """Apply an append_unique patch to a yaml file and return unified diff."""
    return ""
