"""Actionable custom exceptions for the PipelineSentinel data pipeline."""

from typing import Any


class PipelineSentinelError(Exception):
    """Base exception for PipelineSentinel errors."""

    pass


class RetryablePipelineError(PipelineSentinelError):
    """Transient errors that are safe to retry."""

    pass


class NonRetryablePipelineError(PipelineSentinelError):
    """Deterministic errors that must fail fast without wasteful retries."""

    pass


class FormatDriftError(NonRetryablePipelineError):
    """Raised when unparseable date or format ratio exceeds allowed threshold."""

    def __init__(
        self,
        column: str,
        unparseable_ratio: float,
        threshold: float,
        accepted_formats: list[str],
        sample_bad_values: list[Any],
        message: str | None = None,
    ):
        self.column = column
        self.unparseable_ratio = unparseable_ratio
        self.threshold = threshold
        self.accepted_formats = accepted_formats
        self.sample_bad_values = sample_bad_values
        msg = message or (
            f"Unparseable format ratio {unparseable_ratio:.2%} in column '{column}' "
            f"exceeds max threshold {threshold:.2%}. Accepted formats: {accepted_formats}. "
            f"Sample bad values: {sample_bad_values[:5]}"
        )
        super().__init__(msg)


class MissingColumnError(NonRetryablePipelineError):
    """Raised when a required canonical column is missing after aliasing."""

    def __init__(
        self,
        missing_column: str,
        available_columns: list[str],
        message: str | None = None,
    ):
        self.missing_column = missing_column
        self.available_columns = available_columns
        msg = message or (
            f"Required column '{missing_column}' missing from input dataset. "
            f"Available columns: {available_columns}"
        )
        super().__init__(msg)


class DataQualityError(NonRetryablePipelineError):
    """Raised when data quality checks fail and block Gold generation."""

    def __init__(
        self,
        check_name: str,
        observed_value: Any,
        threshold_value: Any,
        message: str | None = None,
    ):
        self.check_name = check_name
        self.observed_value = observed_value
        self.threshold_value = threshold_value
        msg = message or (
            f"Data quality check '{check_name}' failed. "
            f"Observed: {observed_value}, Threshold: {threshold_value}"
        )
        super().__init__(msg)


class AuthError(NonRetryablePipelineError):
    """Raised when authentication to external sources or storage fails."""

    pass
