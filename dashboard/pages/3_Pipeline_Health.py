"""Page 3: Pipeline Health & Observability Metrics."""

import pandas as pd
import streamlit as st

from dashboard.lib.delta_reader import get_gold_refresh_time
from dashboard.lib.ops_db import OpsDbAdapter

st.set_page_config(page_title="Pipeline Health | PipelineSentinel", layout="wide", page_icon="🏥")

st.title("🏥 Pipeline Health & Quality Observability")
st.markdown(
    "Track run reliability, execution durations, retry resiliency, and data quality check results over time."
)

ops = OpsDbAdapter()
runs = ops.get_run_history(limit=100)
dq_list = ops.get_dq_results(limit=100)
resolved_by_retry = ops.get_resolved_by_retry_count()
last_refresh = get_gold_refresh_time()

# 1. Health KPI Summary Cards
kcol1, kcol2, kcol3, kcol4, kcol5 = st.columns(5)

total_runs = len(runs)
success_runs = sum(1 for r in runs if r.get("status") in ("SUCCESS", "RESOLVED", "success"))
success_rate = (success_runs / total_runs * 100.0) if total_runs > 0 else 100.0

total_quarantine = sum(int(r.get("rows_quarantined", 0) or 0) for r in runs)

with kcol1:
    st.metric("Total Executions", f"{total_runs}")
with kcol2:
    st.metric("Success Rate", f"{success_rate:.1f}%")
with kcol3:
    st.metric(
        "Resolved by Retry",
        f"{resolved_by_retry}",
        help="Transient errors absorbed on automatic retry",
    )
with kcol4:
    st.metric(
        "Quarantined Rows", f"{total_quarantine:,}", help="Rows routed to quarantine partition"
    )
with kcol5:
    fresh_str = last_refresh.strftime("%Y-%m-%d %H:%M") if last_refresh else "N/A"
    st.metric("Data Freshness", fresh_str, help="Most recent Gold table commit timestamp")

st.divider()

# 2. Execution Durations & Run History
st.subheader("⏱️ Execution Run History")
if runs:
    df_runs = pd.DataFrame(runs)
    df_runs["started_at"] = pd.to_datetime(df_runs["started_at"])
    df_runs["ended_at"] = pd.to_datetime(df_runs["ended_at"])
    df_runs["duration_sec"] = (
        (df_runs["ended_at"] - df_runs["started_at"]).dt.total_seconds().fillna(0.0)
    )

    # Line chart of durations
    chart_data = df_runs.sort_values(by="started_at").dropna(subset=["started_at"])
    if not chart_data.empty:
        st.caption("Task Run Durations (Seconds)")
        st.line_chart(chart_data.set_index("started_at")["duration_sec"], height=200)

    st.dataframe(
        df_runs[
            [
                "dag_run_id",
                "task_id",
                "batch_date",
                "status",
                "rows_in",
                "rows_out",
                "rows_quarantined",
                "error_type",
                "started_at",
            ]
        ],
        use_container_width=True,
        height=250,
    )
else:
    st.info("No run logs found in `pipeline_run_log`. Run pipeline tasks to populate run history.")

st.divider()

# 3. Data Quality Results Over Time
st.subheader("🛡️ Data Quality Checks Over Time")
if dq_list:
    df_dq = pd.DataFrame(dq_list)
    df_dq["passed_icon"] = df_dq["passed"].apply(lambda p: "✅ PASS" if p else "❌ FAIL")

    qcol1, qcol2 = st.columns([1, 1])
    with qcol1:
        dq_counts = df_dq["passed_icon"].value_counts().reset_index()
        st.caption("Check Status Breakdown")
        st.bar_chart(dq_counts.set_index("passed_icon")["count"])

    with qcol2:
        st.caption("Quarantined Records vs Safe Ingestion")
        if runs:
            quar_by_batch = (
                pd.DataFrame(runs)
                .groupby("batch_date")[["rows_out", "rows_quarantined"]]
                .sum()
                .reset_index()
            )
            st.bar_chart(quar_by_batch.set_index("batch_date"), height=250)

    st.dataframe(
        df_dq[
            [
                "batch_date",
                "table_name",
                "check_name",
                "passed_icon",
                "observed_value",
                "threshold_value",
                "details",
            ]
        ],
        use_container_width=True,
        height=250,
    )
else:
    st.info("No Data Quality check records found in `dq_results`.")
