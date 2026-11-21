"""Unit tests for vendor feed simulator and failure injection."""

import csv
import io
import json
from pathlib import Path

from simulator.failures import FailureMode, apply_failure_to_orders
from simulator.generate_history import generate_history
from simulator.publish_batch import publish_batch

SAMPLE_CSV = """order_id,customer_id,order_date,amount,status,channel
ORD-20261001-00001,CUST-00001,2026-10-01,100.50,placed,web
ORD-20261001-00002,CUST-00002,2026-10-01,45.00,shipped,mobile
ORD-20261001-00003,CUST-00003,2026-10-01,12.99,placed,store
ORD-20261001-00004,CUST-00004,2026-10-01,89.00,placed,partner
ORD-20261001-00005,CUST-00005,2026-10-01,200.00,placed,web
"""


def test_failure_column_rename():
    """Verify COLUMN_RENAME changes 'amount' header to 'total_amount'."""
    transformed = apply_failure_to_orders(SAMPLE_CSV, FailureMode.COLUMN_RENAME)
    reader = csv.DictReader(io.StringIO(transformed))
    assert "total_amount" in reader.fieldnames
    assert "amount" not in reader.fieldnames
    rows = list(reader)
    assert len(rows) == 5
    assert rows[0]["total_amount"] == "100.50"


def test_failure_date_format():
    """Verify DATE_FORMAT converts dates to DD/MM/YYYY with day > 12."""
    # Create larger sample to test percentage injection
    lines = ["order_id,customer_id,order_date,amount,status,channel\n"] + [
        f"ORD-{i:05d},CUST-00001,2026-10-01,50.00,placed,web\n" for i in range(100)
    ]
    large_csv = "".join(lines)
    transformed = apply_failure_to_orders(large_csv, FailureMode.DATE_FORMAT, seed=42)
    reader = csv.DictReader(io.StringIO(transformed))
    drifted_count = 0
    for row in reader:
        val = row["order_date"]
        if "/" in val:
            drifted_count += 1
            parts = val.split("/")
            day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
            assert day > 12, f"Day must be strictly > 12 for unambiguous date drift: {val}"
            assert month == 10
            assert year == 2026

    assert drifted_count > 0, "Expected some dates to drift"


def test_failure_ambiguous_date():
    """Verify AMBIGUOUS_DATE produces slash dates with day <= 12 and month <= 12."""
    lines = ["order_id,customer_id,order_date,amount,status,channel\n"] + [
        f"ORD-{i:05d},CUST-00001,2026-10-01,50.00,placed,web\n" for i in range(100)
    ]
    large_csv = "".join(lines)
    transformed = apply_failure_to_orders(large_csv, FailureMode.AMBIGUOUS_DATE, seed=42)
    reader = csv.DictReader(io.StringIO(transformed))
    ambiguous_count = 0
    for row in reader:
        val = row["order_date"]
        if "/" in val:
            ambiguous_count += 1
            parts = val.split("/")
            day, month, _year = int(parts[0]), int(parts[1]), int(parts[2])
            assert day <= 12, f"Day must be <= 12 for ambiguous date: {val}"
            assert month <= 12, f"Month must be <= 12 for ambiguous date: {val}"

    assert ambiguous_count > 0, "Expected some dates to become ambiguous"


def test_failure_null_spike():
    """Verify NULL_SPIKE blanks out about 30 percent of amount values."""
    lines = ["order_id,customer_id,order_date,amount,status,channel\n"] + [
        f"ORD-{i:05d},CUST-00001,2026-10-01,50.00,placed,web\n" for i in range(100)
    ]
    large_csv = "".join(lines)
    transformed = apply_failure_to_orders(large_csv, FailureMode.NULL_SPIKE, seed=42)
    reader = csv.DictReader(io.StringIO(transformed))
    null_count = sum(1 for row in reader if row["amount"] == "")
    assert 15 <= null_count <= 45, f"Expected ~30% nulls, got {null_count}"


def test_generate_history(tmp_path: Path):
    """Verify generate_history creates valid daily orders and customer files."""
    days = 3
    orders_per_day = 100
    generate_history(
        days=days,
        orders_per_day=orders_per_day,
        seed=123,
        start_date="2026-10-01",
        target_dir=tmp_path,
    )

    for i in range(days):
        date_str = f"2026-10-0{i + 1}"
        orders_file = tmp_path / f"orders_{date_str}.csv"
        cust_file = tmp_path / f"customers_{date_str}.json"

        assert orders_file.exists()
        assert cust_file.exists()

        with open(orders_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == orders_per_day
            assert reader.fieldnames == [
                "order_id",
                "customer_id",
                "order_date",
                "amount",
                "status",
                "channel",
            ]

        with open(cust_file, encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
            assert len(lines) > 0
            first_cust = json.loads(lines[0])
            assert "customer_id" in first_cust
            assert "email" in first_cust


def test_publish_batch(tmp_path: Path):
    """Verify publish_batch copies files and handles missing_file mode."""
    archive = tmp_path / "archive"
    landing = tmp_path / "landing"
    archive.mkdir()
    landing.mkdir()

    batch_date = "2026-10-01"
    orders_src = archive / f"orders_{batch_date}.csv"
    cust_src = archive / f"customers_{batch_date}.json"

    orders_src.write_text(SAMPLE_CSV, encoding="utf-8")
    cust_src.write_text(
        json.dumps({"customer_id": "CUST-00001", "name": "Alice"}) + "\n",
        encoding="utf-8",
    )

    # Normal publish
    publish_batch(batch_date, FailureMode.NONE, archive_dir=archive, landing_dir=landing)
    assert (landing / f"orders_{batch_date}.csv").exists()
    assert (landing / f"customers_{batch_date}.json").exists()

    # Missing file publish
    publish_batch(batch_date, FailureMode.MISSING_FILE, archive_dir=archive, landing_dir=landing)
    assert not (landing / f"orders_{batch_date}.csv").exists()
    assert not (landing / f"customers_{batch_date}.json").exists()
