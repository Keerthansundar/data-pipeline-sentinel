"""Verify idempotency by running the same batch twice."""

from pipeline.bronze import ingest_bronze
from pipeline.dq import run_dq_checks
from pipeline.gold import build_gold
from pipeline.silver import transform_silver
from simulator.failures import FailureMode
from simulator.publish_batch import publish_batch, record_demo_config


def main() -> None:
    batch_date = "2026-10-01"
    print(f"=== Verifying Idempotency on Batch {batch_date} ===")

    publish_batch(batch_date, FailureMode.NONE)
    record_demo_config("none", batch_date)

    print("\n--- Pass 1 ---")
    b1 = ingest_bronze(batch_date)
    s1 = transform_silver(batch_date)
    dq1 = run_dq_checks(batch_date)
    g1 = build_gold(batch_date)
    print(
        f"Pass 1: Bronze={b1['orders_rows']}, Silver={s1['rows_out']}, "
        f"Quarantine={s1['rows_quarantined']}, Gold Rev=${g1['total_revenue']:,.2f}"
    )

    print("\n--- Pass 2 (Rerun) ---")
    b2 = ingest_bronze(batch_date)
    s2 = transform_silver(batch_date)
    dq2 = run_dq_checks(batch_date)
    g2 = build_gold(batch_date)
    print(
        f"Pass 2: Bronze={b2['orders_rows']}, Silver={s2['rows_out']}, "
        f"Quarantine={s2['rows_quarantined']}, Gold Rev=${g2['total_revenue']:,.2f}"
    )

    assert b1["orders_rows"] == b2["orders_rows"], "Bronze row count mismatch"
    assert s1["rows_out"] == s2["rows_out"], "Silver row count mismatch"
    assert s1["rows_quarantined"] == s2["rows_quarantined"], "Quarantine row count mismatch"
    assert dq1["passed"] and dq2["passed"], "Data Quality checks failed"
    assert g1["total_revenue"] == g2["total_revenue"], "Gold revenue mismatch"
    print("\nSUCCESS: Batch rerun is completely idempotent (identical row counts and revenue).")


if __name__ == "__main__":
    main()
