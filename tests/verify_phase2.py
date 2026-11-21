"""Verification script for Phase 2 Checkpoint."""

import csv
from pathlib import Path

from simulator.failures import FailureMode
from simulator.publish_batch import publish_batch


def verify_phase2():
    landing = Path("data/landing")
    batch = "2026-10-01"

    # 1. column_rename
    publish_batch(batch, FailureMode.COLUMN_RENAME)
    with open(landing / f"orders_{batch}.csv", encoding="utf-8") as f:
        hdr = f.readline().strip().split(",")
        assert "total_amount" in hdr
        assert "amount" not in hdr
        print(f"PASS: COLUMN_RENAME (header={hdr})")

    # 2. date_format
    publish_batch(batch, FailureMode.DATE_FORMAT)
    with open(landing / f"orders_{batch}.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        slash_dates = [r["order_date"] for r in reader if "/" in r["order_date"]]
        assert len(slash_dates) > 0
        for d in slash_dates:
            day = int(d.split("/")[0])
            assert day > 12
        print(f"PASS: DATE_FORMAT (count={len(slash_dates)}, sample={slash_dates[:3]})")

    # 3. null_spike
    publish_batch(batch, FailureMode.NULL_SPIKE)
    with open(landing / f"orders_{batch}.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        null_amounts = sum(1 for r in reader if not r["amount"])
        assert 1000 <= null_amounts <= 2000
        print(f"PASS: NULL_SPIKE (null_count={null_amounts}/5000, pct={null_amounts / 50:.1f}%)")

    # 4. ambiguous_date
    publish_batch(batch, FailureMode.AMBIGUOUS_DATE)
    with open(landing / f"orders_{batch}.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        slash_dates = [r["order_date"] for r in reader if "/" in r["order_date"]]
        assert len(slash_dates) > 0
        for d in slash_dates:
            parts = [int(p) for p in d.split("/")]
            assert parts[0] <= 12 and parts[1] <= 12
        print(f"PASS: AMBIGUOUS_DATE (count={len(slash_dates)}, sample={slash_dates[:3]})")

    # 5. missing_file
    publish_batch(batch, FailureMode.MISSING_FILE)
    assert not (landing / f"orders_{batch}.csv").exists()
    assert not (landing / f"customers_{batch}.json").exists()
    print("PASS: MISSING_FILE (files suppressed from landing)")

    # 6. Reset to healthy
    publish_batch(batch, FailureMode.NONE)
    assert (landing / f"orders_{batch}.csv").exists()
    assert (landing / f"customers_{batch}.json").exists()
    print("PASS: NONE (healthy batch restored)")


if __name__ == "__main__":
    verify_phase2()
