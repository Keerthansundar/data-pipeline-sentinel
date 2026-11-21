"""Publish a batch from source_archive to data/landing with optional failure injection."""

import argparse

from simulator.failures import FailureMode


def publish_batch(batch_date: str, failure: FailureMode = FailureMode.NONE) -> None:
    """Publish batch files to data/landing applying failure transformation."""
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish vendor batch")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--failure", default="none", choices=[m.value for m in FailureMode])
    args = parser.parse_args()
    publish_batch(args.batch_date, FailureMode(args.failure))
