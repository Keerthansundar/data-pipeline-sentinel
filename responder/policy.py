"""Patch policy enforcement: file allow-list, key path, whitelist, ambiguity checks."""

import csv
import re
from typing import Any

from pipeline.config import get_project_root, load_yaml_config
from responder.schemas import IncidentDiagnosis, ProposedAction, RootCauseCategory

DATE_FORMAT_WHITELIST = [
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d.%m.%Y",
]

ALLOWED_FILES = ["config/silver_rules.yml"]
ALLOWED_OPERATIONS = ["append_unique"]
ALLOWED_CANONICAL_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "status",
    "channel",
]


def compute_source_ambiguity(batch_date: str) -> float | None:
    """Compute day/month ambiguity ratio directly from landing or source_archive CSV.

    Among order_date values matching ^\\d{2}/\\d{2}/\\d{4}, returns:
    (count where both first and second part <= 12) / (count of slash values).
    Returns 0.0 if there are no slash values.
    Returns None if the source file is missing or unreadable.
    """
    root = get_project_root()
    landing_path = root / "data" / "landing" / f"orders_{batch_date}.csv"
    archive_path = root / "data" / "source_archive" / f"orders_{batch_date}.csv"

    file_path = None
    if landing_path.is_file():
        file_path = landing_path
    elif archive_path.is_file():
        file_path = archive_path
    else:
        return None

    slash_re = re.compile(r"^\d{2}/\d{2}/\d{4}")
    try:
        with open(file_path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "order_date" not in reader.fieldnames:
                return None
            slash_count = 0
            ambiguous_count = 0
            for row in reader:
                val = (row.get("order_date") or "").strip()
                if slash_re.match(val):
                    slash_count += 1
                    parts = val.split("/")
                    if len(parts) >= 2:
                        try:
                            p1, p2 = int(parts[0]), int(parts[1])
                            if p1 <= 12 and p2 <= 12:
                                ambiguous_count += 1
                        except ValueError:
                            pass
            if slash_count == 0:
                return 0.0
            return float(ambiguous_count / slash_count)
    except Exception:
        return None


def load_guardrails_config() -> dict[str, Any]:
    """Load policy configuration from config/guardrails.yml."""
    try:
        return load_yaml_config("config/guardrails.yml").get("agent", {})
    except Exception:
        return {}


def enforce_patch_policy(
    diagnosis: IncidentDiagnosis,
    ambiguous_ratio: float = 0.0,
    batch_date: str | None = None,
) -> tuple[bool, str | None]:
    """Check if proposed diagnosis and patch conform to safety policies.

    Args:
        diagnosis: Validated IncidentDiagnosis.
        ambiguous_ratio: Ambiguous day/month share for the batch date.
        batch_date: Optional batch date string (YYYY-MM-DD). When provided and patch targets
            order_date format, ambiguity is computed directly from source CSV file.

    Returns:
        Tuple of (is_allowed, rejection_reason).
    """
    # 1. Escalations without patches are always safe
    if diagnosis.proposed_action == ProposedAction.ESCALATE:
        return True, None

    # 2. Rules patch must include patch object
    if not diagnosis.patch:
        return False, "Proposed action is 'rules_patch' but no patch descriptor was provided"

    patch = diagnosis.patch
    cfg = load_guardrails_config()
    allowed_files = cfg.get("allowed_files", ALLOWED_FILES)
    allowed_ops = cfg.get("allowed_operations", ALLOWED_OPERATIONS)
    date_whitelist = cfg.get("date_format_whitelist", DATE_FORMAT_WHITELIST)
    min_confidence = float(cfg.get("min_confidence_for_patch", 0.80))
    max_ambiguity = float(cfg.get("max_ambiguous_day_month_share", 0.00))

    # 3. Allow-listed file check
    if patch.file not in allowed_files:
        return (
            False,
            f"Policy violation: Target file '{patch.file}' is not in allow-list {allowed_files}",
        )

    # 4. Operation check
    if patch.operation not in allowed_ops:
        return (
            False,
            f"Policy violation: Operation '{patch.operation}' is not allowed (must be 'append_unique')",
        )

    # 5. Category check: must be format_drift or schema_drift
    if diagnosis.root_cause_category not in [
        RootCauseCategory.FORMAT_DRIFT,
        RootCauseCategory.SCHEMA_DRIFT,
    ]:
        return (
            False,
            f"Policy violation: Automated patches only permitted for format_drift or schema_drift, "
            f"not '{diagnosis.root_cause_category.value}'",
        )

    # 6. Minimum confidence check
    if diagnosis.confidence < min_confidence:
        return (
            False,
            f"Policy violation: Diagnosis confidence {diagnosis.confidence:.2f} is below minimum threshold {min_confidence:.2f}",
        )

    # 7. Key path and whitelist verification
    key_path = patch.key_path.strip()
    val = patch.value.strip()

    if key_path == "orders.date_formats.order_date":
        effective_ambiguity = ambiguous_ratio
        if batch_date is not None:
            source_ambiguity = compute_source_ambiguity(batch_date)
            if source_ambiguity is None:
                return (
                    False,
                    "Policy violation: source file unavailable, cannot verify date ambiguity. Manual escalation required.",
                )
            effective_ambiguity = source_ambiguity

        # Check ambiguity rule
        if effective_ambiguity > max_ambiguity:
            return (
                False,
                f"Policy violation: Date format patch blocked due to ambiguity share {effective_ambiguity:.4f} > {max_ambiguity:.4f}. Manual escalation required.",
            )

        if val not in date_whitelist:
            return (
                False,
                f"Policy violation: Date format '{val}' is not in approved whitelist {date_whitelist}",
            )

    elif key_path.startswith("orders.column_aliases."):
        canonical_col = key_path.replace("orders.column_aliases.", "").strip()
        if canonical_col not in ALLOWED_CANONICAL_COLUMNS:
            return (
                False,
                f"Policy violation: Target alias column '{canonical_col}' is not a valid canonical column {ALLOWED_CANONICAL_COLUMNS}",
            )
        # Value must be a valid identifier and not canonical itself
        if val == canonical_col:
            return (
                False,
                f"Policy violation: Alias '{val}' cannot be identical to canonical column '{canonical_col}'",
            )
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$", val):
            return (
                False,
                f"Policy violation: Alias name '{val}' contains invalid characters or formatting",
            )
    else:
        return (
            False,
            f"Policy violation: Key path '{key_path}' is not an authorized patch target",
        )

    return True, None
