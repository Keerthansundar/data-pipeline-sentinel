"""PipelineSentinel Streamlit Application Main Hub."""

import streamlit as st

from dashboard.lib.ui_components import apply_theme

st.set_page_config(
    page_title="PipelineSentinel Dashboard",
    layout="wide",
    page_icon="🛡️",
    initial_sidebar_state="expanded",
)
apply_theme()

st.title("PipelineSentinel: Self-Healing Medallion Data Pipeline")
st.markdown(
    "Welcome to **PipelineSentinel**, an enterprise self-healing data pipeline featuring Delta Lake medallion architecture, "
    "Airflow orchestration, and a guardrailed OpenAI incident responder agent."
)

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Key Architectural Capabilities")
    st.markdown("""
    - **Bronze, Silver, Gold on Delta Lake:** Daily ETL ingestion, rules-driven schema and date parsing, quarantine partitioning, and aggregated business metrics.
    - **Airflow Orchestration (Airflow 3.4.0):** LocalExecutor with task retries, timeout sensors, failure callbacks, and deterministic stage execution.
    - **Autonomous Incident Agent:** Read-only inspection tools (`fetch_task_error_log`, `inspect_column_profile`, `read_silver_rules`, etc.) citing verbatim evidence.
    - **Isolated Sandbox Verification:** Proposed patches tested against raw data in `data/sandbox/<id>/` before opening pull requests.
    - **Human-in-the-Loop Safeguards:** Agent never merges, never touches `main`, and can only propose structured `append_unique` edits to `config/silver_rules.yml`.
    """)

with col2:
    st.subheader("Navigation Guide")
    st.markdown("""
    Select a page from the sidebar to inspect the live platform:

    1. **[Live Run Monitor](Live_Run_Monitor):** Airflow 'Task Instances' style view with colored pipeline stage graph, progress bar, search/filter, and logs.
    2. **[Revenue Dashboard](Revenue_Dashboard):** Business KPIs, revenue by day, revenue by channel, and customer breakdowns from Delta Lake.
    3. **[Pipeline Health](Pipeline_Health):** Execution run history, retry resilience metrics, data quality checks, and quarantine counts.
    4. **[Incident Audit](Incident_Audit):** Complete post-mortem audit trail, agent tool-call timeline, sandbox verification, and patch diffs.
    5. **[Demo Control](Demo_Control):** Interactive failure injection center with one-click configuration and PowerShell commands.
    """)

st.divider()

st.caption("PipelineSentinel MVP | Built for Google DeepMind Agent Evaluation")
