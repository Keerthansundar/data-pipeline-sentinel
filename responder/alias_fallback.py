"""Deterministic alias-patch proposal for a single-column rename (code, not LLM)."""
import ast
import re

_MISSING_RE = re.compile(
    r"Required column '([^']+)' missing.*?Available columns:\s*(\[[^\]]*\])", re.S
)


def propose_alias_patch(error_text, required_columns, existing_aliases, allowed_targets=("amount",)):
    """Return an append_unique patch dict, or None if the case is not clearly safe."""
    m = _MISSING_RE.search(error_text or "")
    if not m:
        return None
    missing = m.group(1)
    if missing not in allowed_targets:
        return None
    try:
        available = ast.literal_eval(m.group(2))
    except (ValueError, SyntaxError):
        return None
    if not isinstance(available, list) or not all(isinstance(c, str) for c in available):
        return None
    required = set(required_columns)
    extras = [c for c in available if c not in required]
    if len(extras) != 1:
        return None  # zero or several candidates: ambiguous, escalate
    candidate = extras[0]
    if candidate in (existing_aliases or {}).get(missing, []):
        return None  # alias already present and still failing: do not loop
    return {
        "file": "config/silver_rules.yml",
        "operation": "append_unique",
        "key_path": f"orders.column_aliases.{missing}",
        "value": candidate,
    }

def apply_alias_fallback(diag_dict, error_text, rules):
    """If the model diagnosed schema_drift but proposed no patch, derive the alias in code.

    Returns (new_diag_dict, applied: bool). Never raises.
    """
    try:
        if diag_dict.get("root_cause_category") != "schema_drift":
            return diag_dict, False
        if diag_dict.get("proposed_action") == "rules_patch" and diag_dict.get("patch"):
            return diag_dict, False
        orders = (rules or {}).get("orders", {})
        patch = propose_alias_patch(
            error_text,
            orders.get("required_columns", []),
            orders.get("column_aliases", {}),
        )
        if not patch:
            return diag_dict, False
        out = dict(diag_dict)
        out["proposed_action"] = "rules_patch"
        out["patch"] = patch
        out["escalation_reason"] = None
        out["recommended_next_steps"] = (
            "Review and merge the PR, run git pull, then rerun the batch. "
            "The alias was derived by a deterministic code fallback, not by the LLM."
        )
        return out, True
    except Exception:
        return diag_dict, False
