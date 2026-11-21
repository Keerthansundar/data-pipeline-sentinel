"""Publish a batch from source_archive to data/landing with optional failure injection."""

import argparse
import os
import shutil
from pathlib import Path

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger
from simulator.failures import FailureMode, apply_failure_to_orders

logger = get_logger(__name__)


def record_demo_config(failure_mode: str, batch_date: str) -> None:
    """Record current active failure mode in ops database demo_config table.

    Args:
        failure_mode: Name of the failure mode.
        batch_date: Target batch date YYYY-MM-DD.
    """
    try:
        import psycopg

        db_host = os.getenv("OPS_DB_HOST", "ops-postgres")
        db_port = os.getenv("OPS_DB_PORT", "5432")
        db_name = os.getenv("OPS_DB_NAME", "ops")
        db_user = os.getenv("OPS_DB_USER", "ops_user")
        db_password = os.getenv("OPS_DB_PASSWORD", "ops_password")

        conn_str = (
            f"host={db_host} port={db_port} dbname={db_name} "
            f"user={db_user} password={db_password} connect_timeout=3"
        )
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO demo_config (id, failure_mode, target_batch_date, updated_at)
                    VALUES (1, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO UPDATE SET
                        failure_mode = EXCLUDED.failure_mode,
                        target_batch_date = EXCLUDED.target_batch_date,
                        updated_at = CURRENT_TIMESTAMP;
                    """,
                    (failure_mode, batch_date),
                )
    except Exception as exc:
        logger.warning("Could not record demo_config in ops DB: %s", exc)


def publish_batch(
    batch_date: str,
    failure: FailureMode | str = FailureMode.NONE,
    archive_dir: Path | None = None,
    landing_dir: Path | None = None,
) -> None:
    """Publish batch files to data/landing applying failure transformation.

    Args:
        batch_date: Batch date string in YYYY-MM-DD format.
        failure: FailureMode to apply.
        archive_dir: Source archive directory. Defaults to data/source_archive.
        landing_dir: Landing directory. Defaults to data/landing.
    """
    mode = FailureMode(failure) if isinstance(failure, str) else failure
    root = get_project_root()
    if archive_dir is None:
        archive_dir = root / "data" / "source_archive"
    if landing_dir is None:
        landing_dir = root / "data" / "landing"

    landing_dir.mkdir(parents=True, exist_ok=True)

    src_orders = archive_dir / f"orders_{batch_date}.csv"
    src_customers = archive_dir / f"customers_{batch_date}.json"

    dest_orders = landing_dir / f"orders_{batch_date}.csv"
    dest_customers = landing_dir / f"customers_{batch_date}.json"

    # Always clean existing landing files for this date first
    if dest_orders.exists():
        dest_orders.unlink()
    if dest_customers.exists():
        dest_customers.unlink()

    # Record active demo config in Ops DB
    record_demo_config(mode.value, batch_date)

    if mode == FailureMode.MISSING_FILE:
        logger.warning(
            "Simulator failure mode 'missing_file' active: suppressing landing files for %s",
            batch_date,
        )
        return

    if not src_orders.exists() or not src_customers.exists():
        raise FileNotFoundError(
            f"Source archive files missing for batch {batch_date} in {archive_dir}. "
            "Please run 'make seed' first."
        )

    # 1. Transform and write orders CSV
    with open(src_orders, encoding="utf-8") as f:
        raw_orders_content = f.read()

    transformed_orders = apply_failure_to_orders(raw_orders_content, mode)

    with open(dest_orders, "w", encoding="utf-8") as f:
        f.write(transformed_orders)

    # 2. Copy customers JSON lines
    shutil.copy2(src_customers, dest_customers)

    logger.info(
        "Published batch for date=%s with failure_mode=%s to %s",
        batch_date,
        mode.value,
        landing_dir,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish vendor batch")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--failure", default="none", choices=[m.value for m in FailureMode])
    args = parser.parse_args()
    publish_batch(args.batch_date, FailureMode(args.failure))
