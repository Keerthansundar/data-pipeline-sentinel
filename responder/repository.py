"""Postgres repository for ops database queries and audit persistence."""

from typing import Any


class OpsRepository:
    """Read/write repository for ops Postgres database."""

    def __init__(self, db_url: str | None = None):
        self.db_url = db_url

    def get_open_incidents(self) -> list[dict[str, Any]]:
        """Fetch open incidents requiring diagnosis."""
        return []
