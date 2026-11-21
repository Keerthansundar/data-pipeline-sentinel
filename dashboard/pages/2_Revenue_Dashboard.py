"""Page 2: Revenue Dashboard (Gold Layer Analytics via Delta Lake)."""

import streamlit as st

from dashboard.lib.delta_reader import (
    get_revenue_kpis,
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
        daily_agg = df_daily.groupby("order_date")["total_revenue"].sum().reset_index()
        daily_agg = daily_agg.sort_values(by="order_date")
        st.line_chart(daily_agg.set_index("order_date")["total_revenue"], height=300)

    with c2:
        st.subheader("🛍️ Revenue by Channel")
        channel_agg = df_daily.groupby("channel")["total_revenue"].sum().reset_index()
        st.bar_chart(channel_agg.set_index("channel")["total_revenue"], height=300)

    # 3. Tables
    st.subheader("📅 Daily Revenue Breakdown")
    st.dataframe(
        df_daily.style.format(
            {
                "total_revenue": "${:,.2f}",
                "avg_order_value": "${:,.2f}",
                "total_orders": "{:,}",
            }
        ),
        use_container_width=True,
        height=250,
    )

    if not df_cust.empty:
        st.subheader("👥 Top Customer Accounts (Gold customer_revenue)")
        top_cust = df_cust.sort_values(by="lifetime_revenue", ascending=False).head(20)
        st.dataframe(
            top_cust.style.format(
                {
                    "lifetime_revenue": "${:,.2f}",
                    "lifetime_orders": "{:,}",
                }
            ),
            use_container_width=True,
            height=250,
        )
