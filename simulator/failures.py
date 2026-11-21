"""Failure injection transformations for simulator."""

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
