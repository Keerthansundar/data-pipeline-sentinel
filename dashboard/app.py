"""PipelineSentinel Streamlit Application."""

import streamlit as st

st.set_page_config(page_title="PipelineSentinel", layout="wide", page_icon="🛡️")

st.title("PipelineSentinel: Self-Healing Medallion Data Pipeline")
st.markdown(
    "Navigate to monitor live runs, examine pipeline health, review incidents, or control demos."
)
