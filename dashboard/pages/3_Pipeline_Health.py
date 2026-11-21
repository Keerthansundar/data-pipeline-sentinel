"""Page 3: Pipeline Health & Observability (Executive Power BI Style)."""

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.lib.delta_reader import get_gold_refresh_time
from dashboard.lib.metrics import (
    compute_batch_durations,
    compute_latest_row_flow,
    compute_pipeline_health_kpis,
    compute_pipeline_status,
    group_runs_by_batch,
)
from dashboard.lib.ops_db import OpsDbAdapter
from dashboard.lib.ui_components import (
    apply_theme,
    kpi_card,
    section_title,
    status_pill,
)

st.set_page_config(page_title="Pipeline Health | PipelineSentinel", layout="wide", page_icon="🏥")
apply_theme()


@st.cache_data(ttl=30)
def load_health_data():
    ops_adapter = OpsDbAdapter()
    return (
        ops_adapter.get_run_history(limit=100),
        ops_adapter.get_dq_results(limit=50),
        ops_adapter.get_resolved_by_retry_count(),
        ops_adapter.get_incidents(status="OPEN"),
    )


runs, dq_list, resolved_by_retry, open_incidents = load_health_data()
last_refresh = get_gold_refresh_time()
open_incident_count = len(open_incidents)

# Compute health KPIs & status
kpis = compute_pipeline_health_kpis(runs, last_refresh, resolved_by_retry)
latest_run = runs[0] if runs else None
status_info = compute_pipeline_status(latest_run, kpis["freshness_hours"], open_incident_count)

# Header: Title and Status Banner
st.markdown("<h1>Pipeline Health</h1>", unsafe_allow_html=True)

banner_class = f"status-banner status-banner-{status_info['kind']}"
banner_html = f"""
<div class="{banner_class}">
    <div>
        <strong style="margin-right: 8px;">{status_info["title"]}</strong>
        <span>{status_info["detail"]}</span>
    </div>
</div>
"""
st.markdown(banner_html, unsafe_allow_html=True)

# Row 1: Five KPI Cards
kcol1, kcol2, kcol3, kcol4, kcol5 = st.columns(5)

with kcol1:
    kpi_card(
        label="Last Run",
        value=kpis["last_run_status"],
        delta=kpis["last_run_time"],
        good_when="up",
    )

with kcol2:
    kpi_card(
        label="Success Rate",
        value=f"{kpis['success_rate']:.1f}%",
        delta="Last 30 runs",
        good_when="up",
    )

with kcol3:
    fresh_val = f"{kpis['freshness_hours']:.1f}h" if kpis["freshness_hours"] is not None else "N/A"
    kpi_card(
        label="Data Freshness",
        value=fresh_val,
        delta="Limit <= 26h",
        good_when="down",
    )

with kcol4:
    kpi_card(
        label="Quarantine Rate",
        value=f"{kpis['quarantine_rate']:.2f}%",
        delta="Latest batch",
        good_when="down",
    )

with kcol5:
    kpi_card(
        label="Resolved by Retry",
        value=f"{kpis['resolved_by_retry']}",
        delta="Automatic recoveries",
        good_when="up",
    )

st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

# Row 2: Run Duration by Batch & Latest Batch Row Flow
r2_col1, r2_col2 = st.columns([1.5, 1.5])

with r2_col1:
    section_title("Run Duration by Batch")
    batch_dur_df = compute_batch_durations(runs)
    if not batch_dur_df.empty and batch_dur_df["total_duration_sec"].sum() > 0:
        dur_chart = (
            alt.Chart(batch_dur_df)
            .mark_bar(
                size=14,
                color="#0B1F4B",
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
            )
            .encode(
                x=alt.X(
                    "batch_date:O",
                    title=None,
                    axis=alt.Axis(
                        labelAngle=-45 if len(batch_dur_df) > 10 else 0,
                        grid=False,
                    ),
                ),
                y=alt.Y(
                    "total_duration_sec:Q",
                    title="Duration (s)",
                    axis=alt.Axis(grid=True, gridColor="#DDE5E8"),
                ),
                tooltip=[
                    alt.Tooltip("batch_date:O", title="Batch"),
                    alt.Tooltip("total_duration_sec:Q", title="Total Duration", format=".1fs"),
                    alt.Tooltip("status:N", title="Status"),
                ],
            )
            .properties(height=230)
            .configure_view(strokeWidth=0)
        )
        st.altair_chart(dur_chart, use_container_width=True)
    else:
        st.info("No run duration data available.")

with r2_col2:
    section_title("Latest Batch Row Flow")
    flow_items = compute_latest_row_flow(runs)
    if flow_items and any(item["count"] > 0 for item in flow_items):
        flow_df = pd.DataFrame(flow_items)
        flow_chart = (
            alt.Chart(flow_df)
            .mark_bar(
                cornerRadiusTopRight=3,
                cornerRadiusBottomRight=3,
            )
            .encode(
                y=alt.Y(
                    "stage:N",
                    sort=["1. Orders In", "2. Clean Orders", "3. Quarantined", "4. Gold Orders"],
                    title=None,
                    axis=alt.Axis(grid=False),
                ),
                x=alt.X(
                    "count:Q",
                    title="Row Count",
                    axis=alt.Axis(grid=True, gridColor="#DDE5E8"),
                ),
                color=alt.Color(
                    "stage:N",
                    scale=alt.Scale(
                        domain=[
                            "1. Orders In",
                            "2. Clean Orders",
                            "3. Quarantined",
                            "4. Gold Orders",
                        ],
                        range=["#0F9D9A", "#1E9E6A", "#D64545", "#0B1F4B"],
                    ),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("stage:N", title="Stage"),
                    alt.Tooltip("count:Q", title="Rows", format=","),
                    alt.Tooltip("pct_of_in:Q", title="Share of In", format=".1f%"),
                ],
            )
            .properties(height=230)
            .configure_view(strokeWidth=0)
        )
        st.altair_chart(flow_chart, use_container_width=True)
    else:
        st.info("No batch row flow available.")

# Row 3: Data Quality Checks & Recent Runs
r3_col1, r3_col2 = st.columns([1.5, 1.5])

with r3_col1:
    section_title("Data Quality Checks (Latest 8)")
    if dq_list:
        seen = set()
        deduped_dq = []
        for r in dq_list:
            key = (
                r.get("check_name"),
                str(r.get("batch_date")),
                str(r.get("observed_value")),
                str(r.get("threshold_value")),
            )
            if key not in seen:
                seen.add(key)
                deduped_dq.append(r)

        latest_dq = deduped_dq[:8]
        dq_display_rows = []
        for row in latest_dq:
            p_val = row.get("passed", False)
            pill = status_pill("Pass", "success") if p_val else status_pill("Fail", "error")
            dq_display_rows.append(
                {
                    "Batch Date": str(row.get("batch_date", "")),
                    "Check": row.get("check_name", ""),
                    "Status": pill,
                    "Observed": str(row.get("observed_value", "")),
                    "Threshold": str(row.get("threshold_value", "")),
                }
            )
        df_dq_disp = pd.DataFrame(dq_display_rows)
        st.write(df_dq_disp.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No Data Quality check records found.")

with r3_col2:
    section_title("Recent Runs (Latest 8)")
    batch_runs = group_runs_by_batch(runs)
    if batch_runs:
        recent_runs = batch_runs[:8]
        run_display_rows = []
        for r in recent_runs:
            st_val = str(r.get("status", "")).upper()
            kind = (
                "success"
                if st_val in ("SUCCESS", "RESOLVED")
                else ("warning" if "RETRY" in st_val else "error")
            )
            pill = status_pill(st_val, kind)

            dur_sec = float(r.get("total_duration_sec") or 0.0)
            dur_str = f"{dur_sec:.1f}s" if dur_sec > 0 else "-"

            run_display_rows.append(
                {
                    "Batch Date": str(r.get("batch_date", "")),
                    "Status": pill,
                    "Duration": dur_str,
                    "Orders In": f"{r.get('orders_in', 0):,}",
                    "Quarantined": f"{r.get('quarantined', 0):,}",
                    "Retries": str(r.get("retries", 0)),
                }
            )
        df_run_disp = pd.DataFrame(run_display_rows)
        st.write(df_run_disp.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.info("No pipeline run records found.")

# Bottom: Collapsed Expander for Technical Details
with st.expander("Technical details", expanded=False):
    if runs:
        df_all_runs = pd.DataFrame(runs)
        cols_available = [
            c
            for c in [
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
            if c in df_all_runs.columns
        ]
        st.dataframe(df_all_runs[cols_available], use_container_width=True, hide_index=True)
    else:
        st.info("No run logs available.")
