"""Failure injection transformations for simulator."""

import csv
import io
import random
from enum import StrEnum


class FailureMode(StrEnum):
    """Allowed failure modes supported by the simulator."""

    NONE = "none"
    TRANSIENT_ERROR = "transient_error"
    MISSING_FILE = "missing_file"
    DATE_FORMAT = "date_format"
    COLUMN_RENAME = "column_rename"
    NULL_SPIKE = "null_spike"
    AMBIGUOUS_DATE = "ambiguous_date"
    AUTH_ERROR = "auth_error"


FAILURE_MODES: list[str] = [m.value for m in FailureMode]


def apply_failure_to_orders(
    csv_content: str,
    failure_mode: FailureMode | str,
    seed: int = 42,
) -> str:
    """Apply failure transformation to orders CSV content.

    Args:
        csv_content: Raw CSV string of orders.
        failure_mode: The failure mode to inject.
        seed: Random seed for deterministic transformation.

    Returns:
        Transformed CSV content as string.
    """
    mode = FailureMode(failure_mode) if isinstance(failure_mode, str) else failure_mode

    if mode in (FailureMode.NONE, FailureMode.TRANSIENT_ERROR, FailureMode.AUTH_ERROR):
        return csv_content

    reader = csv.DictReader(io.StringIO(csv_content))
    fieldnames = list(reader.fieldnames or [])
    rows = list(reader)

    rng = random.Random(seed)

    if mode == FailureMode.COLUMN_RENAME:
        # Rename 'amount' column to 'total_amount'
        new_fieldnames = ["total_amount" if col == "amount" else col for col in fieldnames]
        out_buf = io.StringIO()
        writer = csv.DictWriter(out_buf, fieldnames=new_fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            new_row = {}
            for col, val in row.items():
                if col == "amount":
                    new_row["total_amount"] = val
                else:
                    new_row[col] = val
            writer.writerow(new_row)
        return out_buf.getvalue()

    out_buf = io.StringIO()
    writer = csv.DictWriter(out_buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()

    for _idx, row in enumerate(rows):
        order_date = row.get("order_date", "")

        if mode == FailureMode.DATE_FORMAT:
            # About 5 percent become DD/MM/YYYY with day > 12 so pattern is clear
            if rng.random() < 0.05 and "-" in order_date:
                parts = order_date.split("-")
                if len(parts) == 3:
                    year, month, _ = parts
                    # Pick a day strictly greater than 12 (13 to 28)
                    day = rng.randint(13, 28)
                    row["order_date"] = f"{day:02d}/{month}/{year}"

        elif mode == FailureMode.AMBIGUOUS_DATE:
            # Slash dates where BOTH day and month are 12 or less
            if rng.random() < 0.05 and "-" in order_date:
                parts = order_date.split("-")
                if len(parts) == 3:
                    year, month, _ = parts
                    m_int = int(month) if int(month) <= 12 else 12
                    day = rng.randint(1, 12)
                    row["order_date"] = f"{day:02d}/{m_int:02d}/{year}"

        elif mode == FailureMode.NULL_SPIKE:
            # About 30 percent of amount blank
            if rng.random() < 0.30:
                row["amount"] = ""

        writer.writerow(row)

    return out_buf.getvalue()
