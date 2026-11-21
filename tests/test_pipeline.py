"""Unit tests for Medallion pipeline logic, rules, and data quality checks."""

import pytest

from pipeline.errors import (
    DataQualityError,
    FormatDriftError,
    MissingColumnError,
)


def test_format_drift_error_fields():
    """Verify FormatDriftError provides actionable context per Section 5A."""
    err = FormatDriftError(
        column="order_date",
        unparseable_ratio=0.052,
        threshold=0.01,
        accepted_formats=["%Y-%m-%d"],
        sample_bad_values=["25/10/2026", "17/10/2026"],
    )
    assert err.column == "order_date"
    assert err.unparseable_ratio == 0.052
    assert err.threshold == 0.01
    assert "25/10/2026" in str(err)
    assert "0.01" in str(err) or "1.00%" in str(err)


def test_missing_column_error_fields():
    """Verify MissingColumnError lists available and missing columns."""
    err = MissingColumnError(
        missing_column="amount",
        available_columns=["order_id", "total_amount", "customer_id"],
    )
    assert err.missing_column == "amount"
    assert "total_amount" in err.available_columns
    assert "amount" in str(err)
    assert "total_amount" in str(err)


def test_data_quality_error_fields():
    """Verify DataQualityError records check name, observed and threshold."""
    err = DataQualityError(
        check_name="max_null_rate:amount",
        observed_value=0.298,
        threshold_value=0.01,
    )
    assert err.check_name == "max_null_rate:amount"
    assert err.observed_value == 0.298
    assert err.threshold_value == 0.01
    assert "max_null_rate:amount" in str(err)


@pytest.mark.spark
def test_spark_pipeline_idempotency_and_silver():
    """Verify bronze -> silver -> dq -> gold execution and idempotency."""
    from pipeline.bronze import ingest_bronze
    from pipeline.dq import run_dq_checks
    from pipeline.gold import build_gold
    from pipeline.silver import transform_silver
    from simulator.failures import FailureMode
    from simulator.publish_batch import publish_batch

    batch_date = "2026-10-01"

    # Ensure clean landing files
    publish_batch(batch_date, FailureMode.NONE)

    # 1. First run
    b1 = ingest_bronze(batch_date)
    s1 = transform_silver(batch_date)
    dq1 = run_dq_checks(batch_date)
    g1 = build_gold(batch_date)

    assert b1["orders_rows"] == 5000
    assert s1["rows_in"] == 5000
    assert s1["rows_out"] + s1["rows_quarantined"] == 5000
    assert dq1["passed"] is True
    assert g1["orders_count"] == s1["rows_out"]

    # 2. Second run (idempotency verification)
    b2 = ingest_bronze(batch_date)
    s2 = transform_silver(batch_date)
    dq2 = run_dq_checks(batch_date)
    g2 = build_gold(batch_date)

    assert b2["orders_rows"] == b1["orders_rows"]
    assert s2["rows_out"] == s1["rows_out"]
    assert s2["rows_quarantined"] == s1["rows_quarantined"]
    assert dq2["passed"] is True
    assert g2["total_revenue"] == g1["total_revenue"]
