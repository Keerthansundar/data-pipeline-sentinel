"""Observability logging and database record helpers."""


def record_pipeline_run(dag_run_id: str, task_id: str, batch_date: str, status: str) -> None:
    """Record pipeline run status into pipeline_run_log table."""
    pass
