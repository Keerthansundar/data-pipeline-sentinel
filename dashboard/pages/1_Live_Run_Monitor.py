"""Page 1: Live Run Monitor (Airflow Task Instances View)."""

import pandas as pd
import streamlit as st

from dashboard.lib.airflow_meta import AirflowMetaAdapter
from dashboard.lib.ops_db import OpsDbAdapter
from dashboard.lib.ui_components import (
    apply_theme,
    generate_pipeline_graph_dot,
    render_status_pill,
)

st.set_page_config(page_title="Live Run Monitor | PipelineSentinel", layout="wide", page_icon="🛡️")
apply_theme()

st.title("🛡️ Live Run Monitor")
st.markdown("Real-time Airflow backend stages, DAG execution graphs, and task instance monitoring.")

# Auto-refresh control in sidebar
with st.sidebar:
    st.header("⚙️ Controls")
    auto_refresh = st.checkbox("Auto-refresh (every 4s)", value=True)
    if auto_refresh:
        st.caption("Auto-refresh active (4 seconds interval)")


@st.fragment(run_every=4 if auto_refresh else None)
def render_live_monitor():
    meta = AirflowMetaAdapter()
    ops = OpsDbAdapter()

    col1, col2 = st.columns([1, 2])
    with col1:
        dags = meta.get_dags()
        default_dag_idx = (
            dags.index("orders_medallion_daily") if "orders_medallion_daily" in dags else 0
        )
        selected_dag = st.selectbox("Select DAG", dags, index=default_dag_idx)

    runs = meta.get_dag_runs(selected_dag, limit=25)
    if not runs:
        st.info(f"No DAG runs found for '{selected_dag}'. Trigger a run to inspect stages.")
        return

    run_options = {
        f"{r['run_id']} ({r.get('state', 'unknown')}) - {r.get('start_date') or r.get('run_after')}": r[
            "run_id"
        ]
        for r in runs
    }
    with col2:
        selected_label = st.selectbox("Select Run", list(run_options.keys()), index=0)
        selected_run_id = run_options[selected_label]

    task_instances = meta.get_task_instances(selected_dag, selected_run_id)

    # 1. Pipeline Status Line and Progress Bar
    total_tasks = len(task_instances)
    completed_tasks = sum(1 for t in task_instances if t.get("State") in ("success", "skipped"))
    failed_tasks = [t for t in task_instances if t.get("State") == "failed"]
    running_tasks = [t for t in task_instances if t.get("State") == "running"]

    if running_tasks:
        cur_t = running_tasks[0]
        status_msg = f"⏳ **Running:** `{cur_t['Task ID']}` (try {cur_t['Try Number']})"
        status_color = "info"
    elif failed_tasks:
        cur_t = failed_tasks[0]
        status_msg = f"❌ **Failed:** `{cur_t['Task ID']}` (try {cur_t['Try Number']})"
        status_color = "error"
    elif total_tasks > 0 and completed_tasks == total_tasks:
        status_msg = f"✅ **Success:** `{selected_dag}` (All {completed_tasks}/{total_tasks} tasks completed)"
        status_color = "success"
    else:
        status_msg = (
            f"ℹ️ **Status:** `{selected_dag}` ({completed_tasks}/{total_tasks} tasks completed)"
        )
        status_color = "warning"

    if status_color == "success":
        st.success(status_msg)
    elif status_color == "error":
        st.error(status_msg)
    elif status_color == "info":
        st.info(status_msg)
    else:
        st.warning(status_msg)

    progress = (completed_tasks / total_tasks) if total_tasks > 0 else 0.0
    st.progress(progress, text=f"Progress: {completed_tasks} of {total_tasks} tasks completed")

    # 2. Colored Pipeline Graph
    st.subheader("📊 Pipeline Stage Graph")
    dot_str = generate_pipeline_graph_dot(task_instances, dag_id=selected_dag)
    st.graphviz_chart(dot_str, use_container_width=True)

    # 3. Tabs: Task Instances, Errors, Logs
    tab_tasks, tab_errors, tab_logs = st.tabs(["📋 Task Instances", "⚠️ Errors", "📜 Logs"])

    with tab_tasks:
        # Search and filter controls
        fcol1, fcol2 = st.columns([2, 1])
        with fcol1:
            search_query = st.text_input("🔍 Search Tasks", placeholder="Filter by task ID...")
        with fcol2:
            state_options = [
                "ALL",
                "success",
                "running",
                "failed",
                "up_for_retry",
                "skipped",
                "queued",
                "upstream_failed",
            ]
            state_filter = st.selectbox("State Filter", state_options, index=0)

        filtered_tasks = task_instances
        if search_query:
            filtered_tasks = [
                t for t in filtered_tasks if search_query.lower() in t["Task ID"].lower()
            ]
        if state_filter != "ALL":
            filtered_tasks = [
                t for t in filtered_tasks if t["State"].lower() == state_filter.lower()
            ]

        if filtered_tasks:
            display_rows = []
            for t in filtered_tasks:
                pill = render_status_pill(t["State"])
                display_rows.append(
                    {
                        "Task ID": t["Task ID"],
                        "Map Index": t["Map Index"],
                        "State": pill,
                        "Start Date": t["Start Date"],
                        "End Date": t["End Date"],
                        "Duration": t["Duration"],
                        "Try Number": t["Try Number"],
                        "Pool": t["Pool"],
                        "Operator": t["Operator"],
                    }
                )
            df_display = pd.DataFrame(display_rows)
            st.write(df_display.to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.info("No task instances match the active search and state filter.")

    with tab_errors:
        if failed_tasks:
            st.error(f"Found {len(failed_tasks)} failed task(s) in run `{selected_run_id}`:")
            for ft in failed_tasks:
                st.markdown(f"### Task: `{ft['Task ID']}` (Try {ft['Try Number']})")
                logs = ops.get_recent_logs(
                    dag_id=selected_dag, task_id=ft["Task ID"], level="ERROR", limit=5
                )
                if logs:
                    for l_row in logs:
                        st.code(
                            f"[{l_row['logged_at']}] {l_row['logger']}\n{l_row['message']}",
                            language="text",
                        )
                else:
                    st.caption(
                        "No specific ERROR level log found in `pipeline_log` table for this task."
                    )
        else:
            st.success("No failed tasks in this DAG run.")

    with tab_logs:
        lcol1, lcol2 = st.columns([2, 1])
        with lcol1:
            log_task_filter = st.selectbox(
                "Filter Log by Task",
                ["ALL"] + [t["Task ID"] for t in task_instances],
            )
        with lcol2:
            log_level_filter = st.selectbox("Log Level", ["ALL", "INFO", "WARNING", "ERROR"])

        logs = ops.get_recent_logs(
            dag_id=selected_dag,
            task_id=log_task_filter if log_task_filter != "ALL" else None,
            level=log_level_filter if log_level_filter != "ALL" else None,
            limit=150,
        )
        if logs:
            log_df = pd.DataFrame(logs)
            log_df["logged_at"] = log_df["logged_at"].astype(str)
            st.dataframe(
                log_df[
                    [
                        "logged_at",
                        "level",
                        "task_id",
                        "batch_date",
                        "try_number",
                        "message",
                    ]
                ],
                use_container_width=True,
                height=350,
            )
        else:
            st.info("No logs found for the selected criteria.")


render_live_monitor()
