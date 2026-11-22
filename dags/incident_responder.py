"""Autonomous incident responder DAG."""

import os
from datetime import UTC, datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.exceptions import AirflowSkipException

from pipeline.config import get_project_root
from pipeline.logging_config import get_logger
from responder.agent import diagnose_incident
from responder.audit import record_incident_audit
from responder.notify import send_incident_email
from responder.patcher import apply_patch_to_file
from responder.policy import enforce_patch_policy
from responder.pr import open_fix_pull_request
from responder.repository import OpsRepository
from responder.sandbox import run_sandbox_verification
from responder.schemas import IncidentDiagnosis
from responder.validate import validate_diagnosis_schema

logger = get_logger(__name__, dag_id="incident_responder")


def _claim_incident(**context: Any) -> str:
    """Find and claim the first OPEN incident. Short-circuit if none exist."""
    repo = OpsRepository()
    open_incidents = repo.get_open_incidents()
    if not open_incidents:
        logger.info("No OPEN incidents requiring diagnosis. Short-circuiting run.")
        raise AirflowSkipException("No OPEN incidents requiring diagnosis.")

    claimed = open_incidents[0]
    inc_id = claimed["id"]
    repo.update_incident_status(inc_id, "IN_PROGRESS")
    logger.info(
        "Claimed incident: %s (batch=%s, task=%s)",
        inc_id,
        claimed["batch_date"],
        claimed["task_id"],
    )
    context["ti"].xcom_push(key="incident_id", value=inc_id)
    context["ti"].xcom_push(key="batch_date", value=claimed["batch_date"])
    return inc_id


def _run_diagnosis_agent(**context: Any) -> dict[str, Any]:
    """Execute autonomous diagnosis agent for the claimed incident."""
    ti = context["ti"]
    inc_id = ti.xcom_pull(task_ids="claim_incident", key="incident_id")
    try:
        diag = diagnose_incident(inc_id)
        diag_dict = diag.model_dump()
        ti.xcom_push(key="diagnosis", value=diag_dict)
        return diag_dict
    except Exception as exc:
        logger.error("Error in diagnosis agent for %s: %s", inc_id, exc)
        diag_dict = {
            "root_cause_category": "unknown",
            "confidence": 0.0,
            "summary": f"Diagnosis agent execution error: {exc}",
            "impact": "Investigation halted",
            "evidence": [],
            "proposed_action": "escalate",
            "patch": None,
            "escalation_reason": f"Exception in agent task: {exc}",
            "recommended_next_steps": "Investigate logs manually.",
        }
        ti.xcom_push(key="diagnosis", value=diag_dict)
        return diag_dict


def _validate_diagnosis(**context: Any) -> dict[str, Any]:
    """Validate schema and citations."""
    ti = context["ti"]
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    valid_schema, diag_obj, schema_err = validate_diagnosis_schema(diag_dict)
    res = {"passed": valid_schema, "error": schema_err}
    ti.xcom_push(key="validation_results", value=res)
    return res


def _enforce_patch_policy(**context: Any) -> dict[str, Any]:
    """Enforce patch grammar, allow-listed files, and ambiguity checks."""
    ti = context["ti"]
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    batch_date = ti.xcom_pull(task_ids="claim_incident", key="batch_date") or ""

    repo = OpsRepository()
    ambiguity_share = 0.0
    for p in repo.get_column_profiles(batch_date):
        if p.get("column_name") == "order_date":
            ambiguity_share = float(p.get("ambiguous_day_month_share", 0.0))
            break

    try:
        diag_obj = IncidentDiagnosis.model_validate(diag_dict)
        policy_valid, policy_err = enforce_patch_policy(
            diag_obj, ambiguity_share, batch_date=batch_date
        )
    except Exception as exc:
        policy_valid, policy_err = False, str(exc)

    res = {"passed": policy_valid, "error": policy_err}
    ti.xcom_push(key="policy_results", value=res)
    return res


def _run_sandbox_verification(**context: Any) -> dict[str, Any]:
    """Run Silver transform on failed batch in sandbox if rules_patch proposed."""
    ti = context["ti"]
    inc_id = ti.xcom_pull(task_ids="claim_incident", key="incident_id")
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    batch_date = ti.xcom_pull(task_ids="claim_incident", key="batch_date")

    action = diag_dict.get("proposed_action")
    patch = diag_dict.get("patch")

    if action != "rules_patch" or not patch:
        res = {"passed": True, "skipped": True}
        ti.xcom_push(key="sandbox_results", value=res)
        return res

    try:
        res = run_sandbox_verification(incident_id=inc_id, patch=patch, batch_date=batch_date)
    except Exception as exc:
        res = {"passed": False, "error": str(exc), "checks": {"no_exception": False}}

    ti.xcom_push(key="sandbox_results", value=res)
    return res


def _route_outcome(**context: Any) -> str:
    """Route decision: open PR if all checks pass, else escalate."""
    ti = context["ti"]
    inc_id = ti.xcom_pull(task_ids="claim_incident", key="incident_id")
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    val_res = ti.xcom_pull(task_ids="validate_diagnosis", key="validation_results") or {}
    pol_res = ti.xcom_pull(task_ids="enforce_patch_policy", key="policy_results") or {}
    sb_res = ti.xcom_pull(task_ids="run_sandbox_verification", key="sandbox_results") or {}

    action = diag_dict.get("proposed_action")
    all_ok = (
        action == "rules_patch"
        and val_res.get("passed", False)
        and pol_res.get("passed", False)
        and sb_res.get("passed", False)
    )

    repo = OpsRepository()
    if all_ok:
        decision = "rules_patch"
        logger.info("Routing decision for %s: Open Fix PR and Notify", inc_id)
    else:
        decision = "escalate"
        repo.update_incident_status(inc_id, "ESCALATED")
        logger.info("Routing decision for %s: Escalate to On-Call Engineer", inc_id)

    ti.xcom_push(key="routing_decision", value=decision)
    return decision


def _open_fix_pull_request(**context: Any) -> str | None:
    """Open PR or write dry_run preview file if routed for fix."""
    ti = context["ti"]
    inc_id = ti.xcom_pull(task_ids="claim_incident", key="incident_id")
    decision = ti.xcom_pull(task_ids="route_outcome", key="routing_decision")
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    sb_res = ti.xcom_pull(task_ids="run_sandbox_verification", key="sandbox_results") or {}

    if decision != "rules_patch":
        logger.info("Skipping PR creation for escalated incident %s", inc_id)
        ti.xcom_push(key="pr_url", value=None)
        return None

    patch = diag_dict.get("patch", {})
    root = get_project_root()
    temp_patched = root / "data" / "sandbox" / inc_id / "silver_rules_patched.yml"

    if temp_patched.exists():
        with open(root / "config" / "silver_rules.yml", encoding="utf-8") as f:
            orig = f.read()
        with open(temp_patched, encoding="utf-8") as f:
            patched = f.read()
        import difflib

        diff = "".join(
            difflib.unified_diff(
                orig.splitlines(keepends=True),
                patched.splitlines(keepends=True),
                fromfile="config/silver_rules.yml",
                tofile="config/silver_rules.yml",
            )
        )
    else:
        diff = apply_patch_to_file(
            root / "config" / "silver_rules.yml",
            temp_patched,
            patch,
        )

    audit_payload = {
        "diagnosis": diag_dict,
        "sandbox_results": sb_res,
    }

    mode, pr_url = open_fix_pull_request(inc_id, diff, audit_payload)
    repo = OpsRepository()
    repo.update_incident_status(inc_id, "PR_OPENED")

    ti.xcom_push(key="pr_url", value=pr_url)
    return pr_url


def _send_incident_email(**context: Any) -> str:
    """Send notification HTML email via Mailpit SMTP sink."""
    ti = context["ti"]
    inc_id = ti.xcom_pull(task_ids="claim_incident", key="incident_id")
    decision = ti.xcom_pull(task_ids="route_outcome", key="routing_decision")
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    sb_res = ti.xcom_pull(task_ids="run_sandbox_verification", key="sandbox_results") or {}
    pr_url = ti.xcom_pull(task_ids="open_fix_pull_request", key="pr_url")

    is_escalation = decision != "rules_patch"

    sent = send_incident_email(
        incident_id=inc_id,
        diagnosis=diag_dict,
        pr_url_or_path=pr_url,
        sandbox_results=sb_res,
        is_escalation=is_escalation,
    )
    status = "SENT" if sent else "FAILED"
    ti.xcom_push(key="email_status", value=status)
    return status


def _record_audit(**context: Any) -> None:
    """Record full investigation audit in incident_diagnosis_audit table."""
    ti = context["ti"]
    inc_id = ti.xcom_pull(task_ids="claim_incident", key="incident_id")
    decision = ti.xcom_pull(task_ids="route_outcome", key="routing_decision") or "escalate"
    diag_dict = ti.xcom_pull(task_ids="run_diagnosis_agent", key="diagnosis") or {}
    val_res = ti.xcom_pull(task_ids="validate_diagnosis", key="validation_results") or {}
    pol_res = ti.xcom_pull(task_ids="enforce_patch_policy", key="policy_results") or {}
    sb_res = ti.xcom_pull(task_ids="run_sandbox_verification", key="sandbox_results")
    pr_url = ti.xcom_pull(task_ids="open_fix_pull_request", key="pr_url")
    email_status = ti.xcom_pull(task_ids="send_incident_email", key="email_status") or "PENDING"

    model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    record_incident_audit(
        incident_id=inc_id,
        diagnosis=diag_dict,
        validation_results=val_res,
        policy_results=pol_res,
        sandbox_results=sb_res,
        final_action="pr_opened" if decision == "rules_patch" else "escalate",
        pr_url=pr_url,
        email_status=email_status,
        model=model_name,
    )


with DAG(
    dag_id="incident_responder",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="* * * * *",
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    tags=["agent", "incident-response"],
) as dag:
    claim_incident = PythonOperator(
        task_id="claim_incident",
        python_callable=_claim_incident,
    )
    run_diagnosis_agent = PythonOperator(
        task_id="run_diagnosis_agent",
        python_callable=_run_diagnosis_agent,
    )
    validate_diagnosis = PythonOperator(
        task_id="validate_diagnosis",
        python_callable=_validate_diagnosis,
    )
    enforce_patch_policy_task = PythonOperator(
        task_id="enforce_patch_policy",
        python_callable=_enforce_patch_policy,
    )
    run_sandbox_verification_task = PythonOperator(
        task_id="run_sandbox_verification",
        python_callable=_run_sandbox_verification,
    )
    route_outcome = PythonOperator(
        task_id="route_outcome",
        python_callable=_route_outcome,
    )
    open_fix_pull_request_task = PythonOperator(
        task_id="open_fix_pull_request",
        python_callable=_open_fix_pull_request,
    )
    send_incident_email_task = PythonOperator(
        task_id="send_incident_email",
        python_callable=_send_incident_email,
    )
    record_audit = PythonOperator(
        task_id="record_audit",
        python_callable=_record_audit,
    )

    (
        claim_incident
        >> run_diagnosis_agent
        >> validate_diagnosis
        >> enforce_patch_policy_task
        >> run_sandbox_verification_task
        >> route_outcome
        >> open_fix_pull_request_task
        >> send_incident_email_task
        >> record_audit
    )
