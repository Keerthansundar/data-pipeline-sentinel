"""Bronze layer raw ingestion."""

from typing import Any


def ingest_bronze(batch_date: str) -> dict[str, Any]:
    """Ingest raw landing files into Bronze Delta tables.

    Args:
        batch_date: Batch date string in YYYY-MM-DD format.

    Returns:
        Summary dict containing rows ingested and metadata.
    """
    return {"batch_date": batch_date, "rows_ingested": 0}
