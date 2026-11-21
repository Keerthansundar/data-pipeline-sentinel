"""Streamlit reusable UI components, styling theme, and Graphviz pipeline visualizers."""

from typing import Any

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
        /* Base background and typography */
        .stApp {
            background-color: #F4F7F8;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            color: #0B1F4B;
        }

        /* Executive Headings */
        h1, h2, h3, h4 {
            color: #0B1F4B !important;
            font-weight: 600 !important;
            letter-spacing: -0.02em;
        }
        h1 { font-size: 1.65rem !important; margin-bottom: 0.2rem !important; }
        h2 { font-size: 1.20rem !important; margin-top: 1rem !important; margin-bottom: 0.4rem !important; }
        h3 { font-size: 1.00rem !important; margin-top: 0.75rem !important; margin-bottom: 0.25rem !important; }

        /* Subtitles & grey text */
        .page-subtitle {
            color: #5B6770;
            font-size: 0.85rem;
            margin-bottom: 1.15rem;
            margin-top: -0.2rem;
        }

        /* KPI Card Container */
        .kpi-container {
            background-color: #FFFFFF;
            border-radius: 8px;
            padding: 14px 18px;
            border: 1px solid #E5E9EB;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
            margin-bottom: 10px;
            min-height: 98px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        .kpi-label {
            color: #5B6770;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            margin-bottom: 4px;
        }
        .kpi-value {
            color: #0B1F4B;
            font-size: 1.60rem;
            font-weight: 700;
            line-height: 1.15;
            margin-bottom: 3px;
        }
        .kpi-delta {
            font-size: 0.74rem;
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .kpi-delta-up-good, .kpi-delta-down-good { color: #1E9E6A; font-weight: 600; }
        .kpi-delta-up-bad, .kpi-delta-down-bad { color: #D64545; font-weight: 600; }
        .kpi-delta-neutral { color: #5B6770; }

        /* Section Titles */
        .section-header {
            color: #0B1F4B;
            font-size: 0.95rem;
            font-weight: 600;
            margin-top: 10px;
            margin-bottom: 8px;
            letter-spacing: -0.01em;
        }

        /* Status Banner */
        .status-banner {
            border-radius: 6px;
            padding: 10px 16px;
            margin-bottom: 14px;
            font-size: 0.875rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .status-banner-healthy {
            background-color: #E6F7EF;
            color: #116943;
            border-left: 4px solid #1E9E6A;
        }
        .status-banner-attention {
            background-color: #FEF3C7;
            color: #92400E;
            border-left: 4px solid #D97706;
        }
        .status-banner-failing {
            background-color: #FEE2E2;
            color: #991B1B;
            border-left: 4px solid #D64545;
        }

        /* Clean Streamlit elements */
        div[data-testid="stDataFrame"] {
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid #E5E9EB;
            background-color: #FFFFFF;
        }
        .streamlit-expanderHeader {
            background-color: #FFFFFF !important;
            border-radius: 6px !important;
            font-weight: 500 !important;
            color: #5B6770 !important;
            font-size: 0.85rem !important;
        }
        div[data-testid="stSegmentedControl"] {
            background-color: #FFFFFF;
            border-radius: 6px;
            border: 1px solid #E5E9EB;
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
    """Return an HTML colored status pill.

    Args:
        text: Badge text.
        kind: 'success', 'warning', 'error', 'info', or 'neutral'.

    Returns:
        HTML string.
    """
    kinds = {
        "success": {"bg": "#E6F7EF", "color": "#116943", "border": "#A7F3D0"},
        "favorable": {"bg": "#E6F7EF", "color": "#116943", "border": "#A7F3D0"},
        "warning": {"bg": "#FEF3C7", "color": "#92400E", "border": "#FDE68A"},
        "amber": {"bg": "#FEF3C7", "color": "#92400E", "border": "#FDE68A"},
        "error": {"bg": "#FEE2E2", "color": "#991B1B", "border": "#FECACA"},
        "unfavorable": {"bg": "#FEE2E2", "color": "#991B1B", "border": "#FECACA"},
        "info": {"bg": "#E0F7F6", "color": "#0B5554", "border": "#B2EBF2"},
        "neutral": {"bg": "#F3F4F6", "color": "#4B5563", "border": "#E5E7EB"},
    }
    cfg = kinds.get(kind.lower(), kinds["neutral"])
    return (
        f'<span style="display:inline-block; padding: 2px 8px; border-radius: 12px; '
        f"font-size: 11px; font-weight: 600; font-family: -apple-system, sans-serif; "
        f'background-color: {cfg["bg"]}; color: {cfg["color"]}; border: 1px solid {cfg["border"]};">'
        f"{text}</span>"
    )


def render_status_pill(state: str | None) -> str:
    """Render an HTML colored pill representing an Airflow task execution state.

    Args:
        state: Airflow task instance state string.

    Returns:
        HTML string for st.markdown.
    """
    st_norm = (state or "queued").lower()
    config = STATE_COLORS.get(st_norm, {"bg": "#6c757d", "text": "#ffffff", "icon": "•"})
    bg = config["bg"]
    color = config["text"]
    icon = config["icon"]

    return (
        f'<span style="display: inline-block; padding: 3px 8px; border-radius: 12px; '
        f"font-size: 11px; font-weight: 600; font-family: monospace; "
        f'background-color: {bg}; color: {color}; text-transform: uppercase;">'
        f"{icon} {st_norm}</span>"
    )


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
