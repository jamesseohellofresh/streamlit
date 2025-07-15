import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from openai import OpenAI
import numpy as np
from pandas.api.types import CategoricalDtype
from streamlit_echarts import st_echarts
from utils.query import (
    run_kraken_raw_data,
    run_kraken_trend_total_cost
)

from utils.commonquery import (
    fetch_hellofresh_weeks,
    blank_repeats,
    format_number_auto
)

from datetime import datetime,timedelta
##from streamlit_autorefresh import st_autorefresh

# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance Portal",
    page_icon=":bulb:",
    layout="wide"
)

# --- Style ---
st.markdown("""
    <style>
        [data-testid="stSidebarNav"] { display: none !important; }
        
    </style>
""", unsafe_allow_html=True)



@st.cache_data(show_spinner="Loading Kraken Ops V3 data...", persist=False)
def load_raw_data(week, version):
    return run_kraken_raw_data(week,version)



@st.cache_data(show_spinner="Loading Kraken Ops V3 Trending data...", persist=True)
def load_trend_data():
    return run_kraken_trend_total_cost()


@st.cache_data(show_spinner=False)
def get_hellofresh_weeks():
    df = fetch_hellofresh_weeks()
    return df['hellofresh_week'].tolist()


st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)


if not st.user.is_logged_in:
    st.switch_page("home.py")



# --- UI ---
st.cache_data.clear()

# 사이드바 상단에 Home 버튼 추가
if st.sidebar.button(f"🏠︎"):
    st.switch_page("home.py")


st.sidebar.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)

# Fetch once and reuse across reruns
hellofresh_weeks = get_hellofresh_weeks()

with st.sidebar:
    hellofresh_week_option = st.selectbox(
        "Hello Fresh Week", 
        options=hellofresh_weeks
    )
    version_option = st.selectbox(
        "Version", 
        options=["v3","v2"]
    )

# Load raw data (cached per version)
df_raw_all = load_raw_data(hellofresh_week_option, version_option)
df_trends = load_trend_data()

df_trend_version= df_trends[df_trends["version"] == version_option]
country_order = CategoricalDtype(["AU", "AO", "NZ"], ordered=True)
df_raw_all["bob_entity_code"] = df_raw_all["bob_entity_code"].astype(country_order)

# --- Summary: Total forecast_total_cost by bob_entity_code ---
summary_df = (
    df_raw_all.groupby("bob_entity_code")["forecast_total_cost"]
    .sum()
    .reset_index()
)

# Format numbers with thousand separator, no decimal
summary_df["forecast_total_cost"] = summary_df["forecast_total_cost"].fillna(0).astype(int).map("{:,}".format)


st.header(f"[{version_option.upper()}] {hellofresh_week_option} Kraken Ops ")

st.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)


cols = st.columns(len(summary_df))

for i, row in summary_df.iterrows():
    cols[i].markdown(f"""
        <div style="background-color:#f8f9fc;padding:1rem;border-radius:10px;
                     box-shadow:0 2px 4px rgba(0,0,0,0.06);text-align:center">
            <div style="font-size:1.1rem;color:#888;">{row['bob_entity_code']}</div>
            <div style="font-size:1.8rem;font-weight:600;color:#2c3e50;">${row['forecast_total_cost']}</div>
        </div>
    """, unsafe_allow_html=True)



# st.dataframe(df_raw_all, use_container_width=True)
trend_summary = (
    df_trend_version.groupby(["hellofresh_week", "bob_entity_code"])["forecast_total_cost"]
    .sum()
    .reset_index()
    .pivot(index="hellofresh_week", columns="bob_entity_code", values="forecast_total_cost")
    .fillna(0)
    .astype(int)
    .sort_index()
)

formatted_df = df_raw_all.copy()

# Apply formatting to all numeric columns
for col in formatted_df.columns:
    if pd.api.types.is_numeric_dtype(formatted_df[col]):
        formatted_df[col] = formatted_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")

with st.expander("🔍 Show details..."):
    st.dataframe(formatted_df, use_container_width=True)

st.markdown(
    """
    <div style="margin-top: 28px; margin-bottom: 38px;"></div>
    """,
    unsafe_allow_html=True
)


st.markdown("### 📈 Weekly Forecast Cost Trends by Entity")

st.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)


for entity in ["AU", "AO", "NZ"]:
    if entity not in trend_summary.columns:
        continue

    values = [
        val if val > 0 else None
        for val in trend_summary[entity].tolist()
    ]

    options = {
        "title": {
            "text": f"{entity} - Forecast Total Cost Trend",
            "textStyle": {"color": "#ffffff"}
        },
        "tooltip": {"trigger": "axis"},
        "legend": {
            "data": [entity],
            "textStyle": {"color": "#ffffff"}
        },
        "xAxis": {
            "type": "category",
            "data": trend_summary.index.tolist(),
            "axisLabel": {"rotate": 45, "color": "#ffffff"},
            "axisLine": {"lineStyle": {"color": "#aaaaaa"}}
        },
        "yAxis": {
            "type": "value",
            "min": "dataMin",
            "max": "dataMax",
            "axisLabel": {"color": "#ffffff"},
            "axisLine": {"lineStyle": {"color": "#aaaaaa"}}
        },
        "series": [{
            "name": entity,
            "type": "line",
            "data": values,
            "smooth": True,
            "connectNulls": False
        }],
        "grid": {"left": "3%", "right": "4%", "bottom": "15%", "containLabel": True}
    }

    st_echarts(options=options, height="400px")


