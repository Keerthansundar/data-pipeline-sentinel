"""Page 2: Revenue Overview (Executive Power BI Style)."""

import altair as alt
import streamlit as st

from dashboard.lib.delta_reader import (
    get_customer_lookup_map,
    get_gold_refresh_time,
    read_gold_channel_revenue,
    read_gold_customer_revenue,
    read_gold_daily_revenue,
)
from dashboard.lib.metrics import (
    compute_channel_scorecard,
    compute_daily_revenue_trend,
    compute_revenue_kpis,
    compute_top_customers,
    filter_by_date_range,
)
from dashboard.lib.ui_components import apply_theme, kpi_card, section_title

st.set_page_config(page_title="Revenue Overview | PipelineSentinel", layout="wide", page_icon="💰")
apply_theme()

# Header: Title, Subtitle, and Date Range Selector
hcol1, hcol2 = st.columns([3, 1])

refresh_time = get_gold_refresh_time()
refresh_str = refresh_time.strftime("%b %d, %Y %H:%M UTC") if refresh_time else "No Gold data yet"

with hcol1:
    st.markdown("<h1>Revenue Overview</h1>", unsafe_allow_html=True)
    st.markdown(
        f'<div class="page-subtitle">Last updated: {refresh_str}</div>',
        unsafe_allow_html=True,
    )

with hcol2:
    if hasattr(st, "segmented_control"):
        selected_range = st.segmented_control(
            "Range",
            ["Last 7 days", "Last 30 days", "All dates"],
            default="All dates",
            label_visibility="collapsed",
        )
    else:
        selected_range = st.selectbox(
            "Range",
            ["All dates", "Last 30 days", "Last 7 days"],
            index=0,
            label_visibility="collapsed",
        )


# Load data with caching
@st.cache_data(ttl=30)
def load_revenue_data():
    return (
        read_gold_daily_revenue(),
        read_gold_channel_revenue(),
        read_gold_customer_revenue(),
    )


df_daily_raw, df_channel_raw, df_cust_raw = load_revenue_data()

if df_daily_raw.empty:
    st.info("Gold tables not yet materialized. Run healthy batches to populate revenue data.")
    st.stop()

# Filter by selected date range
cur_daily, prior_daily = filter_by_date_range(df_daily_raw, selected_range, date_col="order_date")
cur_channel, prior_channel = filter_by_date_range(
    df_channel_raw, selected_range, date_col="_batch_date"
)
cur_cust, prior_cust = filter_by_date_range(df_cust_raw, selected_range, date_col="_batch_date")

kpis = compute_revenue_kpis(cur_daily, cur_cust, prior_daily, prior_cust)

# Row 1: KPI Cards
kcol1, kcol2, kcol3, kcol4 = st.columns(4)

with kcol1:
    kpi_card(
        label="Total Revenue",
        value=f"${kpis['total_revenue']:,.2f}",
        delta=kpis["revenue_var"],
        good_when="up",
    )

with kcol2:
    kpi_card(
        label="Total Orders",
        value=f"{kpis['total_orders']:,}",
        delta=kpis["orders_var"],
        good_when="up",
    )

with kcol3:
    kpi_card(
        label="Average Order Value",
        value=f"${kpis['avg_order_value']:,.2f}",
        delta=kpis["aov_var"],
        good_when="up",
    )

with kcol4:
    kpi_card(
        label="Active Customers",
        value=f"{kpis['active_customers']:,}",
        delta=kpis["customers_var"],
        good_when="up",
    )

st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)

# Row 2: Daily Revenue Trend & Revenue by Channel Donut
r2_col1, r2_col2 = st.columns([1.8, 1.2])

with r2_col1:
    section_title("Daily Revenue")
    trend_df = compute_daily_revenue_trend(cur_daily)
    if not trend_df.empty:
        base = alt.Chart(trend_df).encode(
            x=alt.X(
                "order_date:T",
                title=None,
                axis=alt.Axis(format="%b %d", labelAngle=0, grid=False),
            )
        )
        bars = base.mark_bar(
            color="#0F9D9A",
            opacity=0.85,
            cornerRadiusTopLeft=3,
            cornerRadiusTopRight=3,
        ).encode(
            y=alt.Y(
                "total_revenue:Q",
                title="Revenue ($)",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format="$,.0f", grid=True, gridColor="#E5E9EB"),
            ),
            tooltip=[
                alt.Tooltip("order_date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
            ],
        )
        line = base.mark_line(color="#0B1F4B", strokeWidth=2.5).encode(
            y=alt.Y("moving_avg_7d:Q", title=None),
            tooltip=[alt.Tooltip("moving_avg_7d:Q", title="7D Avg", format="$,.2f")],
        )
        daily_chart = (bars + line).properties(height=260).configure_view(strokeWidth=0)
        st.altair_chart(daily_chart, use_container_width=True)
    else:
        st.info("Daily revenue trend not available.")

with r2_col2:
    section_title("Revenue by Channel")
    if not cur_channel.empty and "channel" in cur_channel.columns:
        ch_rev_col = (
            "channel_revenue" if "channel_revenue" in cur_channel.columns else "total_revenue"
        )
        ch_summary = cur_channel.groupby("channel")[ch_rev_col].sum().reset_index()
        tot_rev = ch_summary[ch_rev_col].sum()
        ch_summary["share_pct"] = (ch_summary[ch_rev_col] / tot_rev * 100.0) if tot_rev > 0 else 0.0

        donut = (
            alt.Chart(ch_summary)
            .mark_arc(innerRadius=50, outerRadius=90, stroke="#FFFFFF", strokeWidth=1.5)
            .encode(
                theta=alt.Theta(f"{ch_rev_col}:Q"),
                color=alt.Color(
                    "channel:N",
                    scale=alt.Scale(range=["#0F9D9A", "#0B1F4B", "#8EDCE0", "#5B6770"]),
                    title=None,
                    legend=alt.Legend(orient="bottom", direction="horizontal"),
                ),
                tooltip=[
                    alt.Tooltip("channel:N", title="Channel"),
                    alt.Tooltip(f"{ch_rev_col}:Q", title="Revenue", format="$,.2f"),
                    alt.Tooltip("share_pct:Q", title="Share", format=".1f%"),
                ],
            )
            .properties(height=260)
            .configure_view(strokeWidth=0)
        )
        st.altair_chart(donut, use_container_width=True)
    else:
        st.info("Channel breakdown not available.")

# Row 3: Top 10 Customers & Channel Scorecard Table
r3_col1, r3_col2 = st.columns([1.4, 1.6])

with r3_col1:
    section_title("Top 10 Customers")
    cust_lookup = get_customer_lookup_map()
    top_cust_df = compute_top_customers(df_cust_raw, customer_names=cust_lookup, limit=10)
    if not top_cust_df.empty:
        cust_bar = (
            alt.Chart(top_cust_df)
            .mark_bar(
                color="#0F9D9A",
                cornerRadiusTopRight=3,
                cornerRadiusBottomRight=3,
            )
            .encode(
                y=alt.Y(
                    "customer_label:N",
                    sort="-x",
                    title=None,
                    axis=alt.Axis(labelLimit=170, grid=False),
                ),
                x=alt.X(
                    "customer_revenue:Q",
                    title="Revenue ($)",
                    axis=alt.Axis(format="$,.0f", grid=True, gridColor="#E5E9EB"),
                ),
                tooltip=[
                    alt.Tooltip("name:N", title="Customer"),
                    alt.Tooltip("customer_id:N", title="ID"),
                    alt.Tooltip("customer_revenue:Q", title="Revenue", format="$,.2f"),
                    alt.Tooltip("total_orders:Q", title="Orders"),
                ],
            )
            .properties(height=250)
            .configure_view(strokeWidth=0)
        )
        st.altair_chart(cust_bar, use_container_width=True)
    else:
        st.info("Customer metrics not available.")

with r3_col2:
    section_title("Channel Scorecard")
    scorecard_df = compute_channel_scorecard(cur_channel, prior_channel=prior_channel)
    if not scorecard_df.empty:
        fmt_map = {
            "Revenue": "${:,.2f}",
            "Orders": "{:,}",
            "Average Order Value": "${:,.2f}",
            "Share %": "{:.1f}%",
        }

        # Format variance column safely
        scorecard_display = scorecard_df.copy()
        scorecard_display["vs Prior Period"] = scorecard_display["vs Prior Period"].apply(
            lambda v: f"{v:+.1f}%" if v is not None else "no prior period"
        )

        st.dataframe(
            scorecard_display.style.format(fmt_map),
            use_container_width=True,
            height=250,
            hide_index=True,
        )
    else:
        st.info("Channel scorecard data not available.")

# Bottom: Collapsed Expander for View Data
with st.expander("View data", expanded=False):
    st.markdown("**Daily Breakdown**")
    date_c = "order_date" if "order_date" in cur_daily.columns else "_batch_date"
    rev_c = "total_revenue" if "total_revenue" in cur_daily.columns else None
    ord_c = "order_count" if "order_count" in cur_daily.columns else "total_orders"
    aov_c = "avg_order_value" if "avg_order_value" in cur_daily.columns else None

    cols_to_show = [c for c in [date_c, rev_c, ord_c, aov_c] if c and c in cur_daily.columns]
    rename_map = {
        date_c: "Batch Date",
        rev_c: "Total Revenue",
        ord_c: "Order Count",
        aov_c: "Average Order Value",
    }
    view_df = cur_daily[cols_to_show].rename(columns=rename_map).sort_values(by="Batch Date")

    view_fmt = {}
    if "Total Revenue" in view_df.columns:
        view_fmt["Total Revenue"] = "${:,.2f}"
    if "Order Count" in view_df.columns:
        view_fmt["Order Count"] = "{:,}"
    if "Average Order Value" in view_df.columns:
        view_fmt["Average Order Value"] = "${:,.2f}"

    st.dataframe(
        view_df.style.format(view_fmt),
        use_container_width=True,
        hide_index=True,
    )
