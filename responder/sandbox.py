"""Isolated sandbox verification for proposed patches on failed batches."""

from typing import Any

import pandas as pd
from ruamel.yaml import YAML

from pipeline.config import get_project_root
from pipeline.errors import FormatDriftError, MissingColumnError
from pipeline.logging_config import get_logger
from responder.patcher import apply_patch_to_file
from responder.repository import OpsRepository

logger = get_logger(__name__)

REQUIRED_SILVER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "status",
    "channel",
]


def execute_sandbox_silver_transform(
    pdf: pd.DataFrame, rules: dict[str, Any]
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Execute Silver transformation logic in sandbox using supplied rules.

    Args:
        pdf: Raw Bronze orders DataFrame.
        rules: Parsed rules dictionary (from patched rules copy).

    Returns:
        Tuple of (silver_pdf, quarantine_pdf, unparseable_ratio).

    Raises:
        MissingColumnError: If required column cannot be resolved.
        FormatDriftError: If unparseable date ratio exceeds threshold.
    """
    orders_rules = rules.get("orders", {})
    required_cols = orders_rules.get("required_columns", [])
    aliases = orders_rules.get("column_aliases", {})
    accepted_formats = orders_rules.get("date_formats", {}).get("order_date", ["%Y-%m-%d"])
    max_unparseable_ratio = float(orders_rules.get("max_unparseable_ratio", 0.01))

    # 1. Required columns & aliases
    data_cols = [c for c in pdf.columns if not c.startswith("_")]
    populated_cols = [c for c in data_cols if pdf[c].notna().sum() > 0]

    for req_col in required_cols:
        is_missing = (req_col not in pdf.columns) or (pdf[req_col].notna().sum() == 0)
        if is_missing:
            col_aliases = aliases.get(req_col, [])
            mapped = False
            for alias in col_aliases:
                if alias in pdf.columns and pdf[alias].notna().sum() > 0:
                    pdf[req_col] = pdf[alias]
                    mapped = True
                    break
            if not mapped:
                raise MissingColumnError(missing_column=req_col, available_columns=populated_cols)

    # 2. Date parsing
    date_series = pdf["order_date"].astype(str).str.strip()
    parsed_s = pd.Series(pd.NaT, index=pdf.index, dtype="datetime64[ns]")

    for fmt in accepted_formats:
        remaining = parsed_s.isna() & ~date_series.isin(["", "None", "nan"])
        if not remaining.any():
            break
        parsed_s[remaining] = pd.to_datetime(date_series[remaining], format=fmt, errors="coerce")

    parse_success = parsed_s.notna()
    parsed_dates = parsed_s.dt.strftime("%Y-%m-%d").where(parse_success, None)

    total_rows = len(pdf)
    unparseable_mask = ~parse_success
    unparseable_count = int(unparseable_mask.sum())
    unparseable_ratio = float(unparseable_count / total_rows) if total_rows > 0 else 0.0

    if unparseable_ratio > max_unparseable_ratio:
        bad_samples = [
            str(v)
            for v in pdf.loc[unparseable_mask, "order_date"].dropna().unique()
            if str(v).strip() not in ("", "None", "nan")
        ][:5]
        raise FormatDriftError(
            column="order_date",
            unparseable_ratio=unparseable_ratio,
            threshold=max_unparseable_ratio,
            accepted_formats=accepted_formats,
            sample_bad_values=bad_samples,
        )

    # 3. Quarantine routing
    pdf["_parsed_order_date"] = parsed_dates
    reasons = pd.Series(None, index=pdf.index, dtype=object)

    reasons.loc[unparseable_mask] = "unparseable_date"

    amt_num = pd.to_numeric(pdf["amount"], errors="coerce")
    reasons.loc[amt_num.isna() & pdf["amount"].notna()] = "invalid_amount_format"
    reasons.loc[amt_num < 0] = "negative_amount"

    null_cust = pdf["customer_id"].isna() | pdf["customer_id"].astype(str).str.strip().isin(
        ["", "None", "nan"]
    )
    reasons.loc[null_cust] = "null_key:customer_id"

    null_order = pdf["order_id"].isna() | pdf["order_id"].astype(str).str.strip().isin(
        ["", "None", "nan"]
    )
    reasons.loc[null_order] = "null_key:order_id"

    quarantined_pdf = pdf[reasons.notna()].copy()
    quarantined_pdf["_quarantine_reason"] = reasons[reasons.notna()]

    silver_pdf = pdf[reasons.isna()].copy()
    silver_pdf = silver_pdf.drop_duplicates(subset=["order_id"])
    silver_pdf["order_date"] = silver_pdf["_parsed_order_date"]
    silver_pdf["amount"] = pd.to_numeric(silver_pdf["amount"], errors="coerce").fillna(0.0)

    return silver_pdf, quarantined_pdf, unparseable_ratio


def verify_sandbox_results(
    bronze_pdf: pd.DataFrame,
    silver_pdf: pd.DataFrame,
    quarantine_pdf: pd.DataFrame,
    unparseable_ratio: float,
    max_unparseable_ratio: float = 0.01,
    rev_tolerance: float = 0.001,
) -> dict[str, Any]:
    """Verify that sandbox execution conforms to all required quality checks.

    Args:
        bronze_pdf: Input raw Bronze DataFrame.
        silver_pdf: Processed Silver DataFrame.
        quarantine_pdf: Quarantined records DataFrame.
        unparseable_ratio: Computed unparseable date fraction.
        max_unparseable_ratio: Configured allowable unparseable limit.
        rev_tolerance: Revenue reconciliation tolerance.

    Returns:
        Detailed dictionary with check results and overall pass/fail status.
    """
    # 1. Unparseable ratio within limit
    unparseable_ok = unparseable_ratio <= max_unparseable_ratio

    # 2. Row count conservation: rows_out + rows_quarantined == rows_in
    rows_in = len(bronze_pdf)
    rows_out = len(silver_pdf)
    rows_q = len(quarantine_pdf)
    conservation_ok = (rows_out + rows_q) == rows_in

    # 3. Revenue reconciliation against raw
    b_amt = pd.to_numeric(bronze_pdf["amount"], errors="coerce").abs().fillna(0.0)
    raw_revenue = float(b_amt.sum())

    s_amt = pd.to_numeric(silver_pdf["amount"], errors="coerce").fillna(0.0)
    silver_revenue = float(s_amt.sum())

    q_amt = pd.to_numeric(quarantine_pdf["amount"], errors="coerce").abs().fillna(0.0)
    quarantine_revenue = float(q_amt.sum())

    reconciled_revenue = silver_revenue + quarantine_revenue
    diff = abs(raw_revenue - reconciled_revenue)
    rel_diff = (diff / raw_revenue) if raw_revenue > 0 else 0.0
    revenue_ok = rel_diff <= rev_tolerance

    # 4. Output schema matches contract
    missing_cols = [c for c in REQUIRED_SILVER_COLUMNS if c not in silver_pdf.columns]
    schema_ok = len(missing_cols) == 0

    all_passed = bool(unparseable_ok and conservation_ok and revenue_ok and schema_ok)

    return {
        "passed": all_passed,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_quarantined": rows_q,
        "unparseable_ratio": unparseable_ratio,
        "raw_revenue": raw_revenue,
        "silver_revenue": silver_revenue,
        "quarantine_revenue": quarantine_revenue,
        "revenue_relative_diff": rel_diff,
        "checks": {
            "no_exception": True,
            "unparseable_ratio_within_limit": unparseable_ok,
            "row_count_conservation": conservation_ok,
            "revenue_reconciles": revenue_ok,
            "output_schema_matches_contract": schema_ok,
        },
    }


def run_sandbox_verification(
    incident_id: str,
    patch: dict[str, Any],
    bronze_pdf: pd.DataFrame | None = None,
    batch_date: str | None = None,
) -> dict[str, Any]:
    """Execute isolated sandbox verification and write results to data/sandbox/<incident_id>/.

    Args:
        incident_id: Unique incident identifier.
        patch: Proposed patch dictionary.
        bronze_pdf: Optional in-memory DataFrame (e.g. for testing).
        batch_date: Target batch date.

    Returns:
        Summary dictionary with verification results.
    """
    root = get_project_root()
    sandbox_dir = root / "data" / "sandbox" / incident_id
    sandbox_dir.mkdir(parents=True, exist_ok=True)

    patched_rules_path = sandbox_dir / "silver_rules_patched.yml"
    original_rules_path = root / "config" / "silver_rules.yml"

    try:
        # 1. Apply patch to temporary copy
        apply_patch_to_file(original_rules_path, patched_rules_path, patch)

        yaml = YAML(typ="safe")
        with open(patched_rules_path, encoding="utf-8") as f:
            patched_rules = yaml.load(f)

        # 2. Acquire Bronze data
        if bronze_pdf is None:
            if not batch_date:
                repo = OpsRepository()
                inc = repo.get_incident(incident_id)
                batch_date = inc.get("batch_date") if inc else None

            if not batch_date:
                return {
                    "passed": False,
                    "error": f"Cannot determine batch date for incident {incident_id}",
                    "checks": {"no_exception": False},
                }

            # Attempt loading Bronze landing/source CSV
            landing_file = root / "data" / "landing" / f"orders_{batch_date}.csv"
            archive_file = root / "data" / "source_archive" / f"orders_{batch_date}.csv"
            src_file = landing_file if landing_file.exists() else archive_file

            if src_file.exists():
                bronze_pdf = pd.read_csv(src_file, dtype=str)
                bronze_pdf["_batch_date"] = batch_date
            else:
                return {
                    "passed": False,
                    "error": f"Bronze source data not found for batch {batch_date}",
                    "checks": {"no_exception": False},
                }

        # 3. Execute sandbox transform (work on a copy; aliases are resolved on it)
        work_pdf = bronze_pdf.copy()
        silver_pdf, quarantine_pdf, unparseable_ratio = execute_sandbox_silver_transform(
            work_pdf, patched_rules
        )

        # 4. Verify results (use the alias-resolved frame so 'amount' exists)
        report = verify_sandbox_results(
            bronze_pdf=work_pdf,
            silver_pdf=silver_pdf,
            quarantine_pdf=quarantine_pdf,
            unparseable_ratio=unparseable_ratio,
        )

        # 5. Write sandbox outputs (NEVER to production tables)
        silver_out = sandbox_dir / "silver_orders.csv"
        quarantine_out = sandbox_dir / "quarantine_orders.csv"
        silver_pdf.to_csv(silver_out, index=False)
        quarantine_pdf.to_csv(quarantine_out, index=False)

        logger.info(
            "Sandbox verification completed for %s: passed=%s, rows_out=%d, rows_q=%d",
            incident_id,
            report["passed"],
            report["rows_out"],
            report["rows_quarantined"],
        )
        return report

    except Exception as exc:
        logger.warning("Sandbox verification failed with exception: %s", exc)
        return {
            "passed": False,
            "error": str(exc),
            "checks": {
                "no_exception": False,
                "unparseable_ratio_within_limit": False,
                "row_count_conservation": False,
                "revenue_reconciles": False,
                "output_schema_matches_contract": False,
            },
        }
