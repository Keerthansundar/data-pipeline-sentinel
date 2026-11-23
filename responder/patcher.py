"""Rules patcher using ruamel.yaml for round-trip preservation and minimal diffs."""

import argparse
import difflib
import json
import sys
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger
from responder.repository import OpsRepository

logger = get_logger(__name__)


def get_yaml_instance() -> YAML:
    """Return configured ruamel.yaml instance preserving round-trip formatting."""
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def apply_patch_to_file(
    source_file: Path,
    dest_file: Path,
    patch: dict[str, Any],
) -> str:
    """Apply an append_unique patch to a copy of a yaml file and return unified diff.

    Args:
        source_file: Path to original YAML file.
        dest_file: Path to write the patched YAML file.
        patch: Patch dictionary or RulesPatch model with file, operation, key_path, value.

    Returns:
        Unified diff string representing the change.
    """
    yaml = get_yaml_instance()

    with open(source_file, encoding="utf-8") as f:
        original_content = f.read()

    data = yaml.load(original_content)
    if data is None:
        data = {}

    key_path = patch.get("key_path", "")
    operation = patch.get("operation", "append_unique")
    value = patch.get("value", "")

    if operation != "append_unique":
        raise ValueError(
            f"Unsupported patch operation: {operation}. Only 'append_unique' is permitted."
        )

    parts = key_path.split(".")
    target = data
    for p in parts[:-1]:
        if p not in target or not isinstance(target[p], dict):
            target[p] = {}
        target = target[p]

    final_key = parts[-1]
    if final_key not in target:
        target[final_key] = [value]
    elif isinstance(target[final_key], list):
        if value not in target[final_key]:
            target[final_key].append(value)
    else:
        # If not a list, convert to list with existing and new value
        existing = target[final_key]
        target[final_key] = [existing, value] if existing != value else [existing]

    dest_file.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_file, "w", encoding="utf-8") as f:
        yaml.dump(data, f)

    with open(dest_file, encoding="utf-8") as f:
        patched_content = f.read()

    diff_lines = list(
        difflib.unified_diff(
            original_content.splitlines(keepends=True),
            patched_content.splitlines(keepends=True),
            fromfile=str(source_file.name),
            tofile=str(dest_file.name),
        )
    )
    diff_str = "".join(diff_lines)
    logger.info(
        "Applied patch to %s -> %s (diff lines: %d)",
        source_file.name,
        dest_file.name,
        len(diff_lines),
    )
    return diff_str


def apply_patch_in_place(file_path: Path, patch: dict[str, Any]) -> str:
    """Apply an append_unique patch directly to a file and return unified diff.

    Args:
        file_path: Target YAML file to modify.
        patch: Patch dictionary.

    Returns:
        Unified diff string.
    """
    temp_copy = file_path.with_suffix(".tmp.yml")
    diff = apply_patch_to_file(file_path, temp_copy, patch)
    temp_copy.replace(file_path)
    return diff


def apply_fix_for_incident(incident_id: str) -> bool:
    """Apply the approved patch for an incident to config/silver_rules.yml.

    Refuses unless the incident is PR_OPENED, the latest audit row is 'pr_opened',
    and the patch policy still approves the stored patch.

    Args:
        incident_id: Target incident ID.

    Returns:
        True if fix applied successfully.
    """
    root = get_project_root()
    rules_file = root / "config" / "silver_rules.yml"

    repo = OpsRepository()
    inc = repo.get_incident(incident_id)
    if not inc:
        print(f"Error: incident {incident_id} not found")
        return False
    if inc.get("status") != "PR_OPENED":
        print(f"Refusing: incident status is {inc.get('status')}, expected PR_OPENED")
        return False

    diag = None
    final_action = None
    conn = repo._get_connection()
    if conn:
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT final_action, diagnosis
                    FROM incident_diagnosis_audit
                    WHERE incident_id = %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1;
                    """,
                    (incident_id,),
                )
                row = cur.fetchone()
                if row:
                    final_action = row[0]
                    diag = json.loads(row[1]) if isinstance(row[1], str) else row[1]
        except Exception as exc:
            logger.warning("Could not read diagnosis from DB: %s", exc)

    if final_action != "pr_opened" or not diag:
        print(f"Refusing: latest audit action is '{final_action}', expected 'pr_opened'")
        return False

    patch_dict = diag.get("patch")
    if not patch_dict:
        print(f"Error: No patch found for incident {incident_id}")
        return False

    from responder.policy import enforce_patch_policy
    from responder.schemas import IncidentDiagnosis

    allowed, reason = enforce_patch_policy(
        IncidentDiagnosis.model_validate(diag), 0.0, batch_date=str(inc.get("batch_date"))
    )
    if not allowed:
        print(f"Refusing: patch policy no longer approves this patch: {reason}")
        return False

    print(f"Applying patch for incident {incident_id} to {rules_file}:")
    print(f"  Key Path: {patch_dict['key_path']}")
    print(f"  Value:    {patch_dict['value']}")

    diff = apply_patch_in_place(rules_file, patch_dict)
    print("\n--- Diff Applied ---")
    print(diff)
    print("SUCCESS: config/silver_rules.yml updated.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply rules patch for an incident")
    parser.add_argument("--incident-id", required=True, help="Incident ID to apply fix for")
    args = parser.parse_args()
    success = apply_fix_for_incident(args.incident_id)
    sys.exit(0 if success else 1)
