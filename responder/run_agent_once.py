"""CLI utility to execute the autonomous diagnosis agent once on a target incident."""

import argparse
import sys

from responder.agent import diagnose_incident
from responder.policy import enforce_patch_policy
from responder.redact import redact_text
from responder.repository import OpsRepository
from responder.validate import validate_evidence_citations


def run_agent_once(incident_id: str) -> int:
    """Run agent on target incident and print formatted investigation report.

    Args:
        incident_id: Incident ID to investigate.

    Returns:
        0 if diagnosis completed, 1 on failure.
    """
    repo = OpsRepository()
    inc = repo.get_incident(incident_id)
    if not inc:
        print(f"Error: Incident '{incident_id}' not found in database.")
        return 1

    print("=" * 70)
    print(f"  INVESTIGATING INCIDENT: {incident_id}")
    print(f"  Task: {inc.get('task_id')} | Batch Date: {inc.get('batch_date')}")
    print(f"  Error Type: {inc.get('error_type')}")
    print(f"  Error Message: {redact_text(inc.get('error_message', ''))[:120]}")
    print("=" * 70)

    diagnosis = diagnose_incident(incident_id)

    # 1. Diagnosis Results
    print("\n[1. DIAGNOSIS]")
    print(f"  Root Cause: {diagnosis.root_cause_category.value}")
    print(f"  Action:     {diagnosis.proposed_action.value}")
    print(f"  Confidence: {diagnosis.confidence:.2f}")
    print(f"  Summary:    {redact_text(diagnosis.summary)}")
    print(f"  Impact:     {redact_text(diagnosis.impact)}")
    if diagnosis.escalation_reason:
        print(f"  Escalation: {redact_text(diagnosis.escalation_reason)}")
    print(f"  Next Steps: {redact_text(diagnosis.recommended_next_steps)}")

    # 2. Evidence Citations
    print("\n[2. EVIDENCE CITATIONS]")
    tool_outputs_map = {}
    conn = repo._get_connection()
    if conn:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT step, tool_name, output_truncated
                        FROM incident_tool_calls
                        WHERE incident_id = %s
                        ORDER BY id ASC;
                        """,
                        (incident_id,),
                    )
                    all_rows = cur.fetchall()
                    start_idx = 0
                    for i, r in enumerate(all_rows):
                        if r[0] == 1:
                            start_idx = i
                    latest_rows = all_rows[start_idx:] if all_rows else []

                    for row in latest_rows:
                        tool_outputs_map[str(row[0])] = row[2] or ""
                        tool_outputs_map[f"call_{row[0]}"] = row[2] or ""
                        tname_clean = (row[1] or "").removeprefix("functions.")
                        if tname_clean in tool_outputs_map:
                            tool_outputs_map[tname_clean] += "\n" + (row[2] or "")
                        else:
                            tool_outputs_map[tname_clean] = row[2] or ""
        except Exception:
            pass

    cit_passed, cit_err = validate_evidence_citations(diagnosis, tool_outputs_map)
    print(f"  Citation Validation: {'PASSED' if cit_passed else f'FAILED ({cit_err})'}")
    if diagnosis.evidence:
        for idx, ev in enumerate(diagnosis.evidence, 1):
            quote = redact_text(ev.quote)
            print(f"  {idx}. [{ev.tool_name}] (Tool Call: {ev.tool_call_id or 'none'})")
            print(f'     Quote: "{quote}"')
    else:
        print("  No evidence items cited.")

    # 3. Patch and Policy Results
    print("\n[3. PROPOSED PATCH & POLICY]")
    if diagnosis.patch:
        print(f"  Target File: {diagnosis.patch.file}")
        print(f"  Operation:   {diagnosis.patch.operation}")
        print(f"  Key Path:    {diagnosis.patch.key_path}")
        print(f"  Value:       {diagnosis.patch.value}")

        if not cit_passed:
            print("  Policy: not evaluated (citation check failed)")
        else:
            ambiguity_share = 0.0
            for p in repo.get_column_profiles(inc.get("batch_date", "")):
                if p.get("column_name") == "order_date":
                    ambiguity_share = float(p.get("ambiguous_day_month_share", 0.0))
                    break

            policy_ok, pol_err = enforce_patch_policy(diagnosis, ambiguity_share)
            print(f"  Policy Pass: {policy_ok} (Ambiguity Share: {ambiguity_share:.4f})")
            if not policy_ok:
                print(f"  Policy Rejection: {pol_err}")
    else:
        print("  No patch proposed (Action: escalate).")

    # 4. Tool Call Timeline
    print("\n[4. TOOL CALL TIMELINE]")
    conn = repo._get_connection()
    tool_calls = []
    if conn:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT step, tool_name, duration_ms, arguments, output_truncated
                        FROM incident_tool_calls
                        WHERE incident_id = %s
                        ORDER BY id ASC;
                        """,
                        (incident_id,),
                    )
                    all_calls = cur.fetchall()
                    start_idx = 0
                    for i, r in enumerate(all_calls):
                        if r[0] == 1:
                            start_idx = i
                    tool_calls = all_calls[start_idx:] if all_calls else []
        except Exception as exc:
            print(f"  Could not load tool call history: {exc}")

    if tool_calls:
        for step, name, dur, args_json, out_trunc in tool_calls:
            print(f"  Step {step}: {name} ({dur}ms)")
            if args_json:
                print(f"    Args: {redact_text(str(args_json))[:80]}")
            snippet = redact_text(out_trunc.strip().replace("\n", " ") if out_trunc else "")[:120]
            print(f"    Output: {snippet}...")
    else:
        print("  No tool calls recorded for this run.")

    # 5. Token Usage and Audit Record
    print("\n[5. AUDIT & TOKENS]")
    conn = repo._get_connection()
    if conn:
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT model, prompt_tokens, completion_tokens, final_action, pr_url_or_preview, email_status, created_at
                        FROM incident_diagnosis_audit
                        WHERE incident_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1;
                        """,
                        (incident_id,),
                    )
                    audit_row = cur.fetchone()
                    if audit_row:
                        print(f"  Model:             {audit_row[0]}")
                        print(f"  Prompt Tokens:     {audit_row[1]}")
                        print(f"  Completion Tokens: {audit_row[2]}")
                        print(f"  Total Tokens:      {audit_row[1] + audit_row[2]}")
                        print(f"  Final Action:      {audit_row[3]}")
                        print(f"  PR / Preview:      {audit_row[4]}")
                        print(f"  Email Status:      {audit_row[5]}")
                        print(f"  Timestamp:         {audit_row[6]}")
        except Exception as exc:
            print(f"  Could not read audit row: {exc}")

    print("=" * 70)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run autonomous diagnosis agent once on an incident"
    )
    parser.add_argument(
        "incident_id", help="Incident ID string (e.g. INC-2026-10-01-silver_transform-...)"
    )
    args = parser.parse_args()
    sys.exit(run_agent_once(args.incident_id))


if __name__ == "__main__":
    main()
