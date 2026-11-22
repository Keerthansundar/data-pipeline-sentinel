"""Page 4: Incident Audit & Autonomous Investigation Inspector."""

import json

import streamlit as st

from dashboard.lib.ops_db import OpsDbAdapter
from dashboard.lib.ui_components import apply_theme, render_status_pill
from responder.redact import redact_text
from responder.validate import validate_evidence_citations

st.set_page_config(page_title="Incident Audit | PipelineSentinel", layout="wide", page_icon="🔍")
apply_theme()

st.title("Incident Audit & Investigation Explorer")
st.markdown(
    "Detailed audit trails for failed pipeline runs, OpenAI agent tool calls, sandbox verification, and patch proposals."
)

ops = OpsDbAdapter()

# 1. Filters and Incident Selection
fcol1, fcol2 = st.columns([1, 2])
with fcol1:
    status_filter = st.selectbox(
        "Filter by Status",
        ["ALL", "OPEN", "IN_PROGRESS", "PR_OPENED", "ESCALATED", "RESOLVED"],
        index=0,
    )

incidents = ops.get_incidents(status=status_filter if status_filter != "ALL" else None)

if not incidents:
    st.info(
        f"No incidents found matching status '{status_filter}'. "
        "The pipeline is currently operating cleanly with zero recorded failures."
    )
    st.stop()

inc_options = {
    f"{inc['id']} | {inc['task_id']} ({inc['status']}) - Batch {inc['batch_date']}": inc["id"]
    for inc in incidents
}

with fcol2:
    selected_label = st.selectbox("Select Incident to Audit", list(inc_options.keys()), index=0)
    selected_inc_id = inc_options[selected_label]

inc = ops.get_incident_detail(selected_inc_id)
audit = ops.get_incident_audit(selected_inc_id)
tool_calls = ops.get_incident_tool_calls(selected_inc_id)

st.divider()

# 2. Incident Summary Header
hcol1, hcol2, hcol3, hcol4 = st.columns(4)
with hcol1:
    st.markdown(f"**Status:** {render_status_pill(inc['status'])}", unsafe_allow_html=True)
with hcol2:
    st.markdown(f"**Failed Task:** `{inc['task_id']}`")
with hcol3:
    st.markdown(f"**Batch Date:** `{inc['batch_date']}`")
with hcol4:
    st.markdown(f"**Error Type:** `{inc.get('error_type', 'Unknown')}`")

with st.expander("🚨 Raw Task Exception & Traceback", expanded=False):
    st.markdown(f"**Error Message:** `{inc.get('error_message')}`")
    if inc.get("error_trace"):
        st.code(inc["error_trace"], language="text")

# 3. Investigation Audit & Agent Diagnosis
st.subheader("🤖 Autonomous Agent Diagnosis & Citations")

diag_data = {}
if audit and audit.get("diagnosis"):
    diag_data = (
        audit["diagnosis"]
        if isinstance(audit["diagnosis"], dict)
        else json.loads(audit["diagnosis"])
    )

if diag_data:
    dcol1, dcol2, dcol3 = st.columns(3)
    with dcol1:
        st.metric("Root Cause Category", diag_data.get("root_cause_category", "unknown").upper())
    with dcol2:
        conf = float(diag_data.get("confidence", 0.0))
        st.metric("Confidence Score", f"{conf:.2f}")
    with dcol3:
        action = diag_data.get("proposed_action", "escalate").upper()
        st.metric("Proposed Action", action)

    st.markdown(f"**Summary:** {redact_text(diag_data.get('summary', ''))}")
    st.markdown(f"**Impact:** {redact_text(diag_data.get('impact', ''))}")
    if diag_data.get("escalation_reason"):
        st.warning(f"**Escalation Reason:** {redact_text(diag_data['escalation_reason'])}")
    if diag_data.get("recommended_next_steps"):
        st.info(f"**Recommended Steps:** {redact_text(diag_data['recommended_next_steps'])}")

    # Evidence & Citation Check
    st.markdown("#### 📜 Cited Evidence & Verification")
    evidence_items = diag_data.get("evidence", [])
    tool_outputs_map = {str(tc["step"]): tc.get("output_truncated", "") for tc in tool_calls}
    for tc in tool_calls:
        tool_outputs_map[f"call_{tc['step']}"] = tc.get("output_truncated", "")

    # Run citation check against recorded tool outputs
    from responder.schemas import IncidentDiagnosis

    try:
        diag_obj = IncidentDiagnosis.model_validate(diag_data)
        cit_ok, cit_err = validate_evidence_citations(diag_obj, tool_outputs_map)
    except Exception as exc:
        cit_ok, cit_err = False, str(exc)

    if cit_ok:
        st.success(
            "✅ **Citation Verification Passed:** All evidence quotes match cited tool outputs verbatim (<=200 chars)."
        )
    else:
        st.error(f"❌ **Citation Verification Failed:** {cit_err}")

    if evidence_items:
        for idx, ev in enumerate(evidence_items, 1):
            st.markdown(
                f"- **Quote {idx}** (`{ev.get('tool_name')}` / ID: `{ev.get('tool_call_id')}`):"
                f' > *"{redact_text(ev.get("quote", ""))}"*'
            )
    else:
        st.caption("No evidence quotes cited in this diagnosis.")

else:
    st.info("No diagnosis audit recorded yet for this incident.")

st.divider()

# 4. Patch Proposal, Policy & Sandbox Verification
st.subheader("🛠️ Patch Proposal, Policy & Sandbox Results")

if diag_data.get("patch"):
    patch = diag_data["patch"]
    pcol1, pcol2 = st.columns(2)
    with pcol1:
        st.markdown(f"- **Target File:** `{patch.get('file')}`")
        st.markdown(f"- **Operation:** `{patch.get('operation')}`")
        st.markdown(f"- **Key Path:** `{patch.get('key_path')}`")
        st.markdown(f"- **Value:** `{patch.get('value')}`")

    with pcol2:
        policy_res = audit.get("policy_results") if audit else {}
        if isinstance(policy_res, str):
            try:
                policy_res = json.loads(policy_res)
            except Exception:
                policy_res = {}
        pol_passed = (
            bool(policy_res.get("approved", policy_res.get("passed", True))) if policy_res else True
        )
        if audit and audit.get("final_action") == "escalate" and diag_data.get("patch"):
            pol_passed = False
        st.metric("Policy Enforcement", "APPROVED" if pol_passed else "REJECTED")
        if policy_res and policy_res.get("reason"):
            st.caption(str(policy_res["reason"]))

    # Sandbox Results
    sb_res = audit.get("sandbox_results") if audit else {}
    if isinstance(sb_res, str):
        try:
            sb_res = json.loads(sb_res)
        except Exception:
            sb_res = {}

    if sb_res:
        st.markdown("#### 🧪 Isolated Sandbox Metrics")
        scol1, scol2, scol3, scol4 = st.columns(4)
        with scol1:
            st.metric("Sandbox Status", "PASSED" if sb_res.get("passed") else "FAILED")
        with scol2:
            st.metric("Rows In", f"{sb_res.get('rows_in', 'N/A')}")
        with scol3:
            st.metric("Rows Out", f"{sb_res.get('rows_out', 'N/A')}")
        with scol4:
            st.metric("Quarantined", f"{sb_res.get('rows_quarantined', 'N/A')}")

    pr_preview = audit.get("pr_url_or_preview") or audit.get("pr_url") if audit else None
    if pr_preview:
        st.markdown(f"**PR / Preview Path:** `{pr_preview}`")

else:
    st.caption("No fix patch proposed for this incident (action was escalation).")

st.divider()

# 5. Agent Tool-Call Timeline
st.subheader("⏳ Agent Tool-Call Timeline")
if tool_calls:
    for tc in tool_calls:
        with st.expander(
            f"Step {tc['step']}: {tc['tool_name']} ({tc['duration_ms']}ms)", expanded=False
        ):
            if tc.get("arguments"):
                st.markdown("**Arguments:**")
                st.json(tc["arguments"])
            st.markdown("**Output (truncated):**")
            st.code(redact_text(tc.get("output_truncated", "")), language="text")
else:
    st.info("No tool calls recorded for this incident.")

st.divider()

# 6. Audit & Token Economics
st.subheader("💵 Investigation Economics & Final Outcome")
if audit:
    acol1, acol2, acol3, acol4 = st.columns(4)
    with acol1:
        st.metric("Model Used", audit.get("model", "N/A"))
    with acol2:
        prompt_t = audit.get("prompt_tokens", 0) or 0
        comp_t = audit.get("completion_tokens", 0) or 0
        st.metric("Total Tokens", f"{prompt_t + comp_t:,}")
    with acol3:
        st.metric("Final Action", audit.get("final_action", "N/A").upper())
    with acol4:
        st.metric("Email Status", audit.get("email_status", "PENDING"))
else:
    st.caption("No audit economics available.")
