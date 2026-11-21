"""Page 2: Revenue Dashboard (Gold Layer Analytics via Delta Lake)."""

import streamlit as st

from dashboard.lib.delta_reader import (
    get_revenue_kpis,
    read_gold_channel_revenue,
    read_gold_customer_revenue,
    read_gold_daily_revenue,
)

st.set_page_config(page_title="Revenue Dashboard | PipelineSentinel", layout="wide", page_icon="💰")

st.title("💰 Revenue Dashboard")
st.markdown(
    "Business KPIs and financial aggregates queried directly from Gold Delta Lake tables without Spark."
)

kpis = get_revenue_kpis()
df_daily = read_gold_daily_revenue()
df_channel = read_gold_channel_revenue()
df_cust = read_gold_customer_revenue()

# 1. KPI Cards
kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

with kpi_col1:
    st.metric(
        label="Total Revenue",
        value=f"${kpis['total_revenue']:,.2f}",
        help="Aggregated revenue from Gold daily_revenue table",
    )
with kpi_col2:
    st.metric(
        label="Total Orders",
        value=f"{kpis['total_orders']:,}",
        help="Total processed clean orders in Gold",
    )
with kpi_col3:
    st.metric(
        label="Average Order Value",
        value=f"${kpis['avg_order_value']:,.2f}",
        help="Total revenue divided by order count",
    )
with kpi_col4:
    refresh_str = (
        kpis["last_refresh"].strftime("%Y-%m-%d %H:%M:%S")
        if kpis["last_refresh"]
        else "No Gold data yet"
    )
    st.metric(
        label="Last Gold Refresh",
        value=refresh_str,
        help="Timestamp of most recent Gold Delta Lake table commit",
    )

st.divider()

if df_daily.empty:
    st.info("Gold tables not yet materialized. Run healthy batches to populate revenue data.")
else:
    # 2. Charts: Revenue by Day & Revenue by Channel
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("📈 Revenue by Day")
        date_col = "order_date" if "order_date" in df_daily.columns else "_batch_date"
        if date_col in df_daily.columns and "total_revenue" in df_daily.columns:
            daily_agg = df_daily.groupby(date_col)["total_revenue"].sum().reset_index()
            daily_agg = daily_agg.sort_values(by=date_col)
            st.line_chart(daily_agg.set_index(date_col)["total_revenue"], height=300)
        else:
            st.info("Daily revenue trend data not available.")

    with c2:
        st.subheader("🛍️ Revenue by Channel")
        channel_source = df_channel if not df_channel.empty else df_daily
        if not channel_source.empty and "channel" in channel_source.columns:
            rev_col = (
                "channel_revenue"
                if "channel_revenue" in channel_source.columns
                else "total_revenue"
            )
            channel_agg = channel_source.groupby("channel")[rev_col].sum().reset_index()
            st.bar_chart(channel_agg.set_index("channel")[rev_col], height=300)
        else:
            st.info("Channel revenue breakdown not available yet.")

    # 3. Tables
    st.subheader("📅 Daily Revenue Breakdown")
    format_dict = {}
    if "total_revenue" in df_daily.columns:
        format_dict["total_revenue"] = "${:,.2f}"
    if "avg_order_value" in df_daily.columns:
        format_dict["avg_order_value"] = "${:,.2f}"
    if "order_count" in df_daily.columns:
        format_dict["order_count"] = "{:,}"
    if "total_orders" in df_daily.columns:
        format_dict["total_orders"] = "{:,}"

    st.dataframe(
        df_daily.style.format(format_dict) if format_dict else df_daily,
        use_container_width=True,
        height=250,
    )

    if not df_cust.empty:
        st.subheader("👥 Top Customer Accounts (Gold customer_summary)")
        sort_col = (
            "customer_revenue"
            if "customer_revenue" in df_cust.columns
            else ("lifetime_revenue" if "lifetime_revenue" in df_cust.columns else None)
        )
        top_cust = (
            df_cust.sort_values(by=sort_col, ascending=False).head(20)
            if sort_col
            else df_cust.head(20)
        )
        cust_fmt = {}
        for c in ["customer_revenue", "lifetime_revenue"]:
            if c in top_cust.columns:
                cust_fmt[c] = "${:,.2f}"
        for c in ["total_orders", "lifetime_orders"]:
            if c in top_cust.columns:
                cust_fmt[c] = "{:,}"

        st.dataframe(
            top_cust.style.format(cust_fmt) if cust_fmt else top_cust,
            use_container_width=True,
            height=250,
        )
    else:
        st.info("Customer summary data not yet materialized.")
