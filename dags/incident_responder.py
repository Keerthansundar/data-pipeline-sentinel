"""Autonomous incident responder DAG."""

from datetime import UTC, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.logging_config import get_logger

logger = get_logger(__name__, dag_id="incident_responder")


def _noop_responder_task(task_name: str):
    logger.info("Executing responder task: %s", task_name)


with DAG(
    dag_id="incident_responder",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["agent", "incident-response"],
) as dag:
    claim_incident = PythonOperator(
        task_id="claim_incident",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "claim_incident"},
    )
    run_diagnosis_agent = PythonOperator(
        task_id="run_diagnosis_agent",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "run_diagnosis_agent"},
    )
    validate_diagnosis = PythonOperator(
        task_id="validate_diagnosis",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "validate_diagnosis"},
    )
    enforce_patch_policy = PythonOperator(
        task_id="enforce_patch_policy",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "enforce_patch_policy"},
    )
    run_sandbox_verification = PythonOperator(
        task_id="run_sandbox_verification",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "run_sandbox_verification"},
    )
    route_outcome = PythonOperator(
        task_id="route_outcome",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "route_outcome"},
    )
    open_fix_pull_request = PythonOperator(
        task_id="open_fix_pull_request",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "open_fix_pull_request"},
    )
    send_incident_email = PythonOperator(
        task_id="send_incident_email",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "send_incident_email"},
    )
    record_audit = PythonOperator(
        task_id="record_audit",
        python_callable=_noop_responder_task,
        op_kwargs={"task_name": "record_audit"},
    )

    (
        claim_incident
        >> run_diagnosis_agent
        >> validate_diagnosis
        >> enforce_patch_policy
        >> run_sandbox_verification
        >> route_outcome
        >> [open_fix_pull_request, send_incident_email]
        >> record_audit
    )
