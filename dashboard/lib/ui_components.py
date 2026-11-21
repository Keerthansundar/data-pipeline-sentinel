"""Streamlit reusable UI components, styling theme, and Graphviz pipeline visualizers."""

from typing import Any

import pandas as pd
import streamlit as st

STATE_COLORS = {
    "success": {"bg": "#28a745", "text": "#ffffff", "icon": "✓"},
    "running": {"bg": "#17a2b8", "text": "#ffffff", "icon": "●"},
    "failed": {"bg": "#dc3545", "text": "#ffffff", "icon": "✗"},
    "up_for_retry": {"bg": "#ffc107", "text": "#212529", "icon": "↻"},
    "skipped": {"bg": "#6c757d", "text": "#ffffff", "icon": "↷"},
    "queued": {"bg": "#adb5bd", "text": "#212529", "icon": "…"},
    "upstream_failed": {"bg": "#e83e8c", "text": "#ffffff", "icon": "⊘"},
}


def apply_theme() -> None:
    """Inject custom CSS for a clean, executive Power BI styling across pages."""
    st.markdown(
        """
        <style>
        /* Base background and typography (at least 15px base text) */
        .stApp {
            background-color: #F4F7F8;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            color: #0B1F4B;
            font-size: 15px;
        }

        /* Top Streamlit header bar (match light page with readable dark text) */
        header[data-testid="stHeader"] {
            background-color: #F4F7F8 !important;
            border-bottom: 1px solid #DDE5E8 !important;
        }
        header[data-testid="stHeader"] * {
            color: #0B1F4B !important;
        }
        [data-testid="stToolbar"] {
            right: 1.5rem !important;
        }
        [data-testid="stToolbar"] button {
            color: #0B1F4B !important;
        }

        /* Sidebar: dark navy with readable text & active highlight */
        section[data-testid="stSidebar"] {
            background-color: #0B1F4B !important;
        }
        section[data-testid="stSidebar"] * {
            color: #E8EEF2 !important;
            font-size: 15px !important;
        }
        section[data-testid="stSidebar"] a {
            color: #E8EEF2 !important;
            border-radius: 6px;
            padding: 6px 12px;
            transition: all 0.15s ease-in-out;
        }
        section[data-testid="stSidebar"] a:hover {
            background-color: #16326E !important;
            color: #FFFFFF !important;
        }
        section[data-testid="stSidebar"] [aria-current="page"] {
            background-color: #17326B !important;
            border-left: 4px solid #0F9D9A !important;
            font-weight: 600 !important;
            color: #FFFFFF !important;
        }

        /* Executive Headings (no emojis, navy #0B1F4B) */
        h1, h2, h3, h4 {
            color: #0B1F4B !important;
            font-weight: 600 !important;
            letter-spacing: -0.02em;
        }
        h1 { font-size: 1.65rem !important; margin-bottom: 0.2rem !important; }
        h2 { font-size: 1.20rem !important; margin-top: 1rem !important; margin-bottom: 0.4rem !important; }
        h3 { font-size: 18px !important; font-weight: 600 !important; margin-top: 0.75rem !important; margin-bottom: 0.25rem !important; }

        /* Subtitles, Captions & "Last updated" (at least 13px in #4A5560) */
        .page-subtitle, .stCaption, small, [data-testid="stCaptionContainer"] {
            color: #4A5560 !important;
            font-size: 13px !important;
        }
        .page-subtitle {
            margin-bottom: 1.15rem;
            margin-top: -0.2rem;
        }

        /* Card Titles and Section Headers (18px, weight 600, navy #0B1F4B) */
        .section-header {
            color: #0B1F4B;
            font-size: 18px;
            font-weight: 600;
            margin-top: 10px;
            margin-bottom: 8px;
            letter-spacing: -0.01em;
        }

        /* KPI Card Container */
        .kpi-container {
            background-color: #FFFFFF;
            border-radius: 8px;
            padding: 16px 20px;
            border: 1px solid #B7C4CA;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
            margin-bottom: 12px;
            min-height: 105px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        .kpi-label {
            color: #3D4852;
            font-size: 13px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 4px;
        }
        .kpi-value {
            color: #0B1F4B;
            font-size: 32px;
            font-weight: 700;
            line-height: 1.15;
            margin-bottom: 3px;
        }
        .kpi-delta {
            font-size: 13px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .kpi-delta-up-good, .kpi-delta-down-good { color: #0B6B45; font-weight: 700; }
        .kpi-delta-up-bad, .kpi-delta-down-bad { color: #A12727; font-weight: 700; }
        .kpi-delta-neutral { color: #4A5560; font-weight: 500; }

        /* Buttons & Form Controls (min 40px height, 1px #B7C4CA border, focus ring) */
        button[kind="primary"], button[kind="secondary"], .stButton > button {
            min-height: 40px !important;
            border-radius: 6px !important;
            border: 1px solid #B7C4CA !important;
            background-color: #FFFFFF !important;
            color: #0B1F4B !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            padding: 6px 16px !important;
            transition: all 0.15s ease-in-out !important;
        }
        .stButton > button:hover {
            background-color: #E3F4F4 !important;
            border-color: #0F9D9A !important;
            color: #0B1F4B !important;
        }
        .stButton > button:focus {
            box-shadow: 0 0 0 2px rgba(15, 157, 154, 0.4) !important;
            border-color: #0F9D9A !important;
        }

        /* Segmented Control (Date Range Selector): clear 1px #B7C4CA border, 40px height */
        div[data-testid="stSegmentedControl"] {
            background-color: #FFFFFF !important;
            border-radius: 6px !important;
            border: 1px solid #B7C4CA !important;
            min-height: 40px !important;
            padding: 2px !important;
        }
        div[data-testid="stSegmentedControl"] button {
            border: none !important;
            background-color: transparent !important;
            color: #0B1F4B !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            min-height: 36px !important;
            padding: 4px 14px !important;
            border-radius: 4px !important;
            transition: all 0.15s ease !important;
        }
        div[data-testid="stSegmentedControl"] button:hover {
            background-color: #E3F4F4 !important;
            color: #0B1F4B !important;
        }
        div[data-testid="stSegmentedControl"] button[aria-checked="true"],
        div[data-testid="stSegmentedControl"] button[data-checked="true"],
        div[data-testid="stSegmentedControl"] button[aria-selected="true"] {
            background-color: #0F9D9A !important;
            color: #FFFFFF !important;
            font-weight: 600 !important;
        }

        /* Selectboxes, text inputs, search boxes */
        div[data-testid="stSelectbox"] > div,
        div[data-testid="stTextInput"] > div > div {
            min-height: 40px !important;
            border: 1px solid #B7C4CA !important;
            border-radius: 6px !important;
            background-color: #FFFFFF !important;
            color: #0B1F4B !important;
            font-size: 14px !important;
        }
        div[data-testid="stSelectbox"] > div:focus-within,
        div[data-testid="stTextInput"] > div > div:focus-within {
            border-color: #0F9D9A !important;
            box-shadow: 0 0 0 2px rgba(15, 157, 154, 0.4) !important;
        }

        /* Expander headers */
        .streamlit-expanderHeader, details summary {
            background-color: #FFFFFF !important;
            border: 1px solid #B7C4CA !important;
            border-radius: 6px !important;
            font-weight: 600 !important;
            color: #0B1F4B !important;
            font-size: 14px !important;
            min-height: 40px !important;
            padding: 8px 14px !important;
        }
        .streamlit-expanderHeader:hover, details summary:hover {
            background-color: #E3F4F4 !important;
            border-color: #0F9D9A !important;
        }

        /* Tables (body 14px, headers 13px bold #EEF3F5) */
        table {
            font-size: 14px !important;
            color: #0B1F4B !important;
        }
        th {
            font-size: 13px !important;
            font-weight: 700 !important;
            background-color: #EEF3F5 !important;
            color: #0B1F4B !important;
        }

        /* Status Banner */
        .status-banner {
            border-radius: 6px;
            padding: 12px 18px;
            margin-bottom: 16px;
            font-size: 14px;
            font-weight: 500;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .status-banner-healthy {
            background-color: #DDF3E8;
            color: #0B6B45;
            border-left: 5px solid #0B6B45;
        }
        .status-banner-attention {
            background-color: #FFF1CC;
            color: #8A5A00;
            border-left: 5px solid #8A5A00;
        }
        .status-banner-failing {
            background-color: #FBE1E1;
            color: #A12727;
            border-left: 5px solid #A12727;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(
    label: str,
    value: Any,
    delta: float | str | None = None,
    delta_label: str | None = None,
    good_when: str = "up",
) -> None:
    """Render a white rounded card with soft shadow, large value, and delta variance.

    Args:
        label: Small grey metric label (uppercase).
        value: Large primary value.
        delta: Numeric percentage variance (e.g. 12.4 for +12.4%) or status string.
        delta_label: Subtitle label beside variance (default 'vs prior period').
        good_when: 'up' (favorable when positive) or 'down' (favorable when negative).
    """
    if delta is not None:
        if isinstance(delta, (int, float)):
            is_positive = delta >= 0
            arrow = "▲" if is_positive else "▼"
            abs_val = abs(delta)

            if good_when == "up":
                delta_class = "kpi-delta-up-good" if is_positive else "kpi-delta-down-bad"
            else:
                delta_class = "kpi-delta-up-bad" if is_positive else "kpi-delta-down-good"

            lbl = delta_label or "vs prior period"
            delta_html = (
                f'<div class="kpi-delta {delta_class}">'
                f"<span>{arrow} {abs_val:.1f}%</span> "
                f'<span style="color:#5B6770;font-weight:normal;">{lbl}</span></div>'
            )
        else:
            delta_html = f'<div class="kpi-delta kpi-delta-neutral">{delta}</div>'
    else:
        delta_html = '<div class="kpi-delta kpi-delta-neutral">&nbsp;</div>'

    card_html = f"""
    <div class="kpi-container">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def section_title(text: str) -> None:
    """Render a clean section title without emojis in navy blue.

    Args:
        text: Heading string.
    """
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


def status_pill(text: str, kind: str = "neutral") -> str:
    """Return an HTML colored status pill with WCAG AA contrast.

    Colors:
        - green: text #0B6B45 on #DDF3E8 (border #B2E5CD)
        - red: text #A12727 on #FBE1E1 (border #F7B5B5)
        - amber: text #8A5A00 on #FFF1CC (border #FCE199)
        - info: text #0B5554 on #D8F3F2 (border #A5E6E4)
        - neutral: text #2B3640 on #EAEFF2 (border #D1DCE2)
    """
    kinds = {
        "success": {"bg": "#DDF3E8", "color": "#0B6B45", "border": "#B2E5CD"},
        "favorable": {"bg": "#DDF3E8", "color": "#0B6B45", "border": "#B2E5CD"},
        "pass": {"bg": "#DDF3E8", "color": "#0B6B45", "border": "#B2E5CD"},
        "resolved": {"bg": "#DDF3E8", "color": "#0B6B45", "border": "#B2E5CD"},
        "warning": {"bg": "#FFF1CC", "color": "#8A5A00", "border": "#FCE199"},
        "amber": {"bg": "#FFF1CC", "color": "#8A5A00", "border": "#FCE199"},
        "retry": {"bg": "#FFF1CC", "color": "#8A5A00", "border": "#FCE199"},
        "up_for_retry": {"bg": "#FFF1CC", "color": "#8A5A00", "border": "#FCE199"},
        "error": {"bg": "#FBE1E1", "color": "#A12727", "border": "#F7B5B5"},
        "unfavorable": {"bg": "#FBE1E1", "color": "#A12727", "border": "#F7B5B5"},
        "fail": {"bg": "#FBE1E1", "color": "#A12727", "border": "#F7B5B5"},
        "failed": {"bg": "#FBE1E1", "color": "#A12727", "border": "#F7B5B5"},
        "info": {"bg": "#D8F3F2", "color": "#0B5554", "border": "#A5E6E4"},
        "neutral": {"bg": "#EAEFF2", "color": "#2B3640", "border": "#D1DCE2"},
    }
    cfg = kinds.get(kind.lower(), kinds["neutral"])
    return (
        f'<span style="display:inline-block; padding: 3px 10px; border-radius: 12px; '
        f"font-size: 13px; font-weight: 700; font-family: -apple-system, BlinkMacSystemFont, sans-serif; "
        f"background-color: {cfg['bg']}; color: {cfg['color']}; border: 1px solid {cfg['border']}; "
        f'letter-spacing: 0.02em;">{text}</span>'
    )


def render_status_pill(state: str | None) -> str:
    """Render an HTML colored pill representing an Airflow task execution state."""
    st_norm = (state or "queued").lower()
    config = STATE_COLORS.get(st_norm, {"bg": "#6c757d", "text": "#ffffff", "icon": "•"})
    bg = config["bg"]
    color = config["text"]
    icon = config["icon"]

    return (
        f'<span style="display: inline-block; padding: 3px 10px; border-radius: 12px; '
        f"font-size: 13px; font-weight: 700; font-family: monospace; "
        f'background-color: {bg}; color: {color}; text-transform: uppercase;">'
        f"{icon} {st_norm}</span>"
    )


def render_html_table(
    df: pd.DataFrame,
    right_align_cols: list[str] | None = None,
    max_height: str | None = None,
) -> str:
    """Render a styled executive HTML table with zebra stripes, hover, and right-aligned numbers.

    Args:
        df: Pandas DataFrame to render.
        right_align_cols: Optional list of column names to right-align.
        max_height: Optional CSS max-height for scrollable table container.

    Returns:
        HTML string.
    """
    if df.empty:
        return (
            '<div style="color: #4A5560; font-size: 14px; padding: 12px;">No data available.</div>'
        )

    right_cols = set(right_align_cols or [])
    if not right_cols:
        for c in df.columns:
            col_str = str(c).lower()
            if pd.api.types.is_numeric_dtype(df[c]) or any(
                term in col_str
                for term in (
                    "revenue",
                    "order",
                    "value",
                    "share",
                    "duration",
                    "count",
                    "quarantined",
                    "observed",
                    "threshold",
                    "retries",
                    "in",
                    "out",
                )
            ):
                right_cols.add(c)

    headers_html = "".join(
        f'<th style="background-color: #EEF3F5; color: #0B1F4B; font-size: 13px; font-weight: 700; '
        f"padding: 10px 14px; text-align: {'right' if col in right_cols else 'left'}; "
        f'border-bottom: 2px solid #DDE5E8; white-space: nowrap;">{col}</th>'
        for col in df.columns
    )

    rows_html = []
    for idx, row in df.iterrows():
        bg = "#FFFFFF" if idx % 2 == 0 else "#F9FBFC"
        cells_html = "".join(
            f'<td style="padding: 9px 14px; font-size: 14px; color: #0B1F4B; '
            f"text-align: {'right' if col in right_cols else 'left'}; "
            f'border-bottom: 1px solid #E5E9EB; white-space: nowrap;">{row[col]}</td>'
            for col in df.columns
        )
        rows_html.append(
            f'<tr style="background-color: {bg}; transition: background-color 0.15s ease;" '
            f"onmouseover=\"this.style.backgroundColor='#F0F4F6'\" "
            f"onmouseout=\"this.style.backgroundColor='{bg}'\">{cells_html}</tr>"
        )

    scroll_style = f"max-height: {max_height}; overflow-y: auto;" if max_height else ""
    table_html = f"""
    <div style="border: 1px solid #B7C4CA; border-radius: 6px; overflow: hidden; background-color: #FFFFFF; {scroll_style}">
        <table style="width: 100%; border-collapse: collapse; font-family: -apple-system, BlinkMacSystemFont, sans-serif;">
            <thead>
                <tr>{headers_html}</tr>
            </thead>
            <tbody>
                {"".join(rows_html)}
            </tbody>
        </table>
    </div>
    """
    return table_html


def generate_pipeline_graph_dot(tasks: list[dict[str, Any]], dag_id: str = "") -> str:
    """Generate Graphviz DOT specification colored by task states.

    Args:
        tasks: List of task instance dictionaries containing 'Task ID' and 'State'.
        dag_id: DAG identifier to apply known canonical topology.

    Returns:
        Graphviz DOT language string.
    """
    task_states = {t.get("Task ID", ""): (t.get("State") or "queued").lower() for t in tasks}

    dag_topologies = {
        "orders_medallion_daily": [
            "wait_for_vendor_files",
            "bronze_ingest",
            "silver_transform",
            "data_quality_checks",
            "gold_build",
            "freshness_check",
            "resolve_incidents",
        ],
        "incident_responder": [
            "claim_incident",
            "run_diagnosis_agent",
            "validate_diagnosis",
            "enforce_patch_policy",
            "run_sandbox_verification",
            "route_outcome",
            "open_fix_pull_request",
            "send_incident_email",
            "record_audit",
        ],
        "vendor_feed_simulator": [
            "simulate_vendor_feed",
        ],
    }

    topology = dag_topologies.get(dag_id, [t.get("Task ID", "") for t in tasks])

    dot_lines = [
        "digraph Pipeline {",
        "  rankdir=LR;",
        '  bgcolor="transparent";',
        '  node [shape=box, style="filled,rounded", fontname="Helvetica,Arial,sans-serif", fontsize=10, penwidth=1.5];',
        '  edge [color="#6c757d", penwidth=1.2, arrowsize=0.8];',
    ]

    for t_id in topology:
        state = task_states.get(t_id, "queued")
        cfg = STATE_COLORS.get(state, {"bg": "#adb5bd", "text": "#212529"})
        dot_lines.append(
            f'  "{t_id}" [label="{t_id}\\n({state})", fillcolor="{cfg["bg"]}", fontcolor="{cfg["text"]}", color="{cfg["bg"]}"];'
        )

    for i in range(len(topology) - 1):
        dot_lines.append(f'  "{topology[i]}" -> "{topology[i + 1]}";')

    dot_lines.append("}")
    return "\n".join(dot_lines)
