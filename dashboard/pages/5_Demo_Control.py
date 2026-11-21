"""Page 5: Interactive Demo Control & Failure Injection Center."""

import streamlit as st

from dashboard.lib.ops_db import OpsDbAdapter

st.set_page_config(page_title="Demo Control | PipelineSentinel", layout="wide", page_icon="🎛️")

st.title("🎛️ Demo Control Center")
st.markdown(
    "Inject deterministic failure modes, update demo configuration, and get exact execution commands."
)

ops = OpsDbAdapter()
current_cfg = ops.get_demo_config()

FAILURE_MODES = [
    ("none", "Healthy baseline execution (all tasks pass, Gold updated)"),
    ("transient_error", "Transient network glitch (fails try 1, passes automatically on try 2)"),
    ("date_format", "Vendor date format drift to DD/MM/YYYY (auto-diagnosed, sandbox-verified PR)"),
    (
        "column_rename",
        "Vendor column drift amount -> total_amount (auto-diagnosed, alias patch PR)",
    ),
    ("null_spike", "Data quality null spike >25% (DQ fails, Gold blocked, email escalation)"),
    (
        "ambiguous_date",
        "Ambiguous dates (e.g. 05/10/2026 - guardrail blocks patch, email escalation)",
    ),
    (
        "auth_error",
        "Source authentication failure (fails fast on try 1, infrastructure escalation)",
    ),
    ("missing_file", "Vendor delivery delay (sensor times out after 120s, late data escalation)"),
]

mode_keys = [m[0] for m in FAILURE_MODES]
mode_descriptions = {m[0]: m[1] for m in FAILURE_MODES}

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("1. Configure Failure Scenario")
    cur_mode = current_cfg.get("failure_mode", "none")
    mode_idx = mode_keys.index(cur_mode) if cur_mode in mode_keys else 0

    selected_mode = st.selectbox(
        "Select Failure Mode",
        mode_keys,
        index=mode_idx,
        format_func=lambda k: f"{k} - {mode_descriptions.get(k, '')}",
    )

    batch_date = st.text_input(
        "Target Batch Date (YYYY-MM-DD)",
        value=current_cfg.get("target_batch_date", "2026-10-01"),
    )

    if st.button("💾 Apply Configuration to Ops DB", type="primary"):
        success = ops.set_demo_config(selected_mode, batch_date)
        if success:
            st.success(f"Updated `demo_config`: mode=`{selected_mode}`, batch=`{batch_date}`")
        else:
            st.error("Failed to update `demo_config` in database.")

with col2:
    st.subheader("2. Scenario Behavior & Expectations")
    st.info(
        f"**Selected Scenario:** `{selected_mode}`\n\n{mode_descriptions.get(selected_mode, '')}"
    )

    st.markdown("#### What to observe in the system:")
    if selected_mode == "none":
        st.markdown("- **Airflow:** `orders_medallion_daily` runs clean across all 7 tasks.")
        st.markdown("- **Gold Layer:** `daily_revenue` and `customer_revenue` updated.")
        st.markdown("- **Incidents:** No incidents created.")
    elif selected_mode == "transient_error":
        st.markdown(
            "- **Airflow:** `bronze_ingest` fails on Try 1 with `RetryablePipelineError`, then succeeds on Try 2."
        )
        st.markdown("- **Incidents:** No incident logged; retry absorbed failure.")
    elif selected_mode == "date_format":
        st.markdown(
            "- **Airflow:** `silver_transform` fails immediately (FormatDriftError) with no retry."
        )
        st.markdown("- **Ops DB:** Incident created with `format_drift`.")
        st.markdown("- **Agent:** Proposes `append_unique` strptime format `%d/%m/%Y`.")
        st.markdown("- **Sandbox:** Verifies clean transform in `data/sandbox/<incident_id>/`.")
        st.markdown(
            "- **PR / Preview:** Writes preview markdown to `data/pr_previews/<incident_id>.md`."
        )
    elif selected_mode == "ambiguous_date":
        st.markdown("- **Airflow:** `silver_transform` fails with FormatDriftError.")
        st.markdown("- **Agent Guardrail:** `ambiguous_day_month_share > 0` blocks date patch.")
        st.markdown("- **Decision:** Escalate to human engineer via email.")
    elif selected_mode == "null_spike":
        st.markdown("- **Airflow:** `data_quality_checks` fails; Gold tasks blocked.")
        st.markdown("- **Decision:** Escalate to human engineer via email.")
    elif selected_mode == "auth_error":
        st.markdown("- **Airflow:** `bronze_ingest` fails immediately on try 1.")
        st.markdown("- **Decision:** Immediate infrastructure escalation email.")
    elif selected_mode == "missing_file":
        st.markdown("- **Airflow:** `wait_for_vendor_files` sensor times out.")
        st.markdown("- **Decision:** Late data escalation email.")

st.divider()

# 3. Execution Commands
st.subheader("3. Execution Commands")
st.markdown(
    "Run either the automated Python test harness or trigger the Airflow DAG directly via PowerShell:"
)

st.markdown("#### Option A: Run Single Failure Mode Demo Harness (Fast)")
cmd_harness = f"docker compose exec airflow-scheduler python -m tests.run_failure_modes_demo --mode {selected_mode} --batch-date {batch_date}"
st.code(cmd_harness, language="powershell")

st.markdown("#### Option B: Trigger Airflow DAG Execution")
cmd_publish = f"docker compose exec airflow-scheduler python -m simulator.publish_batch --failure={selected_mode} --batch-date={batch_date}"
cmd_trigger = f'docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf \'{{"batch_date": "{batch_date}"}}\''
st.code(f"{cmd_publish}\n{cmd_trigger}", language="powershell")

if selected_mode in ("date_format", "column_rename"):
    st.markdown("#### Option C: Apply Automated Fix & Rerun (Human-in-the-loop Simulation)")
    st.code(
        "# 1. Apply rules fix for incident:\n"
        "python -m responder.patcher --incident-id <INCIDENT_ID>\n\n"
        "# 2. Rerun batch to resolve incident and refresh Gold:\n"
        f'docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf \'{{"batch_date": "{batch_date}"}}\'',
        language="powershell",
    )
