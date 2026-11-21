"""Structured logging framework adhering to Section 5A of the project brief."""

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

from responder.redact import redact_text

DEFAULT_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "dag=%(dag_id)s task=%(task_id)s run_id=%(run_id)s batch=%(batch_date)s try=%(try_number)s | "
    "%(message)s"
)


class ContextFilter(logging.Filter):
    """Ensures default context fields are present on every LogRecord."""

    def __init__(self, context: dict[str, Any] | None = None):
        super().__init__()
        self.context = context or {}

    def filter(self, record: logging.LogRecord) -> bool:
        # Default fallback values for context
        record.dag_id = getattr(record, "dag_id", self.context.get("dag_id", "-"))
        record.task_id = getattr(record, "task_id", self.context.get("task_id", "-"))
        record.run_id = getattr(record, "run_id", self.context.get("run_id", "-"))
        record.batch_date = getattr(record, "batch_date", self.context.get("batch_date", "-"))
        record.try_number = getattr(record, "try_number", self.context.get("try_number", "-"))
        return True


class OpsDbLogHandler(logging.Handler):
    """Persists log records (INFO and above) to the Postgres pipeline_log table.

    Fail-safe: catches all exceptions, never blocks tasks, truncates messages to 2,000
    characters, and applies redaction before persisting.
    """

    def __init__(self, level: int = logging.INFO):
        super().__init__(level)
        self._db_connected: bool | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Lazy import to avoid circular dependency or import failures when ops DB isn't up
            import psycopg

            db_host = os.getenv("OPS_DB_HOST", "ops-postgres")
            db_port = os.getenv("OPS_DB_PORT", "5432")
            db_name = os.getenv("OPS_DB_NAME", "ops")
            db_user = os.getenv("OPS_DB_USER", "ops_user")
            db_password = os.getenv("OPS_DB_PASSWORD", "ops_password")

            raw_msg = self.format(record) if self.formatter else record.getMessage()
            cleaned_msg = redact_text(raw_msg)[:2000]

            conn_str = (
                f"host={db_host} port={db_port} dbname={db_name} "
                f"user={db_user} password={db_password} connect_timeout=2"
            )

            context_dict = {
                "pathname": record.pathname,
                "lineno": record.lineno,
                "funcName": record.funcName,
            }
            if record.exc_text:
                context_dict["exc_text"] = redact_text(record.exc_text)[:2000]

            with psycopg.connect(conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO pipeline_log (
                            logged_at, level, logger, dag_id, task_id, run_id,
                            batch_date, try_number, message, context
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            datetime.now(UTC),
                            record.levelname,
                            record.name,
                            str(getattr(record, "dag_id", "-")),
                            str(getattr(record, "task_id", "-")),
                            str(getattr(record, "run_id", "-")),
                            str(getattr(record, "batch_date", "-")),
                            str(getattr(record, "try_number", "-")),
                            cleaned_msg,
                            json.dumps(context_dict),
                        ),
                    )
        except Exception:
            # Per brief: OpsDbLogHandler must never raise and never block a task
            pass


def get_logger(name: str, **context: Any) -> logging.LoggerAdapter:
    """Return a standard Python logging adapter enriched with pipeline context.

    Args:
        name: The logger name (typically __name__).
        **context: Context keys like dag_id, task_id, run_id, batch_date, try_number.

    Returns:
        logging.LoggerAdapter configured with console and fail-safe DB handlers.
    """
    logger = logging.getLogger(name)

    # Only add handlers if not already configured
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # Console handler with stdout output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(DEFAULT_FORMAT)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Ops DB handler (INFO and above)
        ops_handler = OpsDbLogHandler(level=logging.INFO)
        logger.addHandler(ops_handler)

    # Attach context filter to populate missing fields
    ctx_filter = ContextFilter(context)
    logger.addFilter(ctx_filter)

    return logging.LoggerAdapter(logger, extra=context)
