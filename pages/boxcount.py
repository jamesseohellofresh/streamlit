import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from openai import OpenAI
import numpy as np
from pandas.api.types import CategoricalDtype
from streamlit_echarts import st_echarts

from utils.boxcountquery import (
    run_box_count_raw
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
        .stMetric {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 10px;
            margin: 0.2rem;
            text-align: center;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
        }
        .stMetric > div {
            font-size: 1rem;
            color: #666;
        }
        .stMetric span {
            font-size: 1.5rem;
            font-weight: 700;
            color: #2c3e50;
        }
            .country-card {
            background-color: #f8f9fc;
            border-radius: 12px;
            padding: 1rem;
            box-shadow: 0 4px 12px rgba(0,0,0,0.06);
            text-align: center;
            margin-bottom: 0.5rem;
            font-size: 0.95rem;
        }
        .country-title {
            font-weight: 700;
            font-size: 0.9rem;
            margin-bottom: 0.3rem;
            color: #7d7d7d;
        }
        .country-value {
            font-size: 1.4rem;
            font-weight: 600;
            color: #2c3e50;
        }
        .delta-pos {
            color: #27ae60;
            font-weight: bold;
        }
        .delta-neg {
            color: #c0392b;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)



@st.cache_data(show_spinner="Loading Box Count data...", persist=True)
def load_box_count_raw_data():
    return run_box_count_raw()

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


# Load raw data (cached per version)
df_raw_all = load_box_count_raw_data()




st.header(f"{hellofresh_week_option} Box Count")

st.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)

# Filter after loading
df_raw = df_raw_all[df_raw_all["hellofresh_week"] == hellofresh_week_option]
source_summary = df_raw.groupby('source')['box_count'].sum().reset_index()

pivot_df = df_raw.pivot_table(
    index='country',
    columns='source',
    values='box_count',
    aggfunc='sum',
    fill_value=0
).reset_index()

country_order = CategoricalDtype(["AU", "AO", "NZ"], ordered=True)
pivot_df["country"] = pivot_df["country"].astype(country_order)
pivot_df = pivot_df.sort_values("country")

pivot_df['FACT_vs_EDW'] = pivot_df.get('FACT', 0) - pivot_df.get('EDW', 0)
pivot_df['ANZ_vs_EDW'] = pivot_df.get('ANZ', 0) - pivot_df.get('EDW', 0)

for _, row in pivot_df.iterrows():
    edw = int(row.get("EDW", 0))
    fact = int(row.get("FACT", 0))
    anz = int(row.get("ANZ", 0))
    fact_delta = int(row.get("FACT_vs_EDW", 0))
    anz_delta = int(row.get("ANZ_vs_EDW", 0))

    fact_delta_html = f"<span class='delta-pos'>(+{fact_delta:,})</span>" if fact_delta >= 0 else f"<span class='delta-neg'>({fact_delta:,})</span>"
    anz_delta_html = f"<span class='delta-pos'>(+{anz_delta:,})</span>" if anz_delta >= 0 else f"<span class='delta-neg'>({anz_delta:,})</span>"

    with st.container():
        col1, col2, col3, col4, col5 = st.columns(5)

        col1.markdown(f"<div class='country-card'><div class='country-title'>Country</div><div class='country-value'>{row['country']}</div></div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='country-card'><div class='country-title'>EDW</div><div class='country-value'>{edw:,}</div></div>", unsafe_allow_html=True)
        col3.markdown(f"<div class='country-card'><div class='country-title'>FACT</div><div class='country-value'>{fact:,} {fact_delta_html}</div></div>", unsafe_allow_html=True)
        col4.markdown(f"<div class='country-card'><div class='country-title'>ANZ</div><div class='country-value'>{anz:,} {anz_delta_html}</div></div>", unsafe_allow_html=True)


# Pivot weekly trends by country and source
weekly_df = (
    df_raw_all.groupby(["hellofresh_week", "country", "source"])["box_count"]
    .sum()
    .reset_index()
    .pivot_table(index="hellofresh_week", columns=["country", "source"], values="box_count", fill_value=0)
    .sort_index()
)


st.markdown(
    """
    <div style="margin-top: 28px; margin-bottom: 38px;"></div>
    """,
    unsafe_allow_html=True
)


# --- Weekly Trend Line Charts per Country ---
st.markdown("## 📈 Weekly Box Count Trends (EDW vs FACT vs ANZ)")

st.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)

# Prepare pivoted weekly data
weekly_df = (
    df_raw_all.groupby(["hellofresh_week", "country", "source"])["box_count"]
    .sum()
    .reset_index()
    .pivot_table(index="hellofresh_week", columns=["country", "source"], values="box_count", fill_value=0)
    .sort_index()
)

# Render one chart per country
for country in ["AU", "AO", "NZ"]:
    weeks = weekly_df.index.tolist()
    series = []

    for source in ["EDW", "FACT", "ANZ"]:
        col_key = (country, source)
        if col_key in weekly_df.columns:
            values = weekly_df[col_key].tolist()
            values = [v if v > 0 else None for v in values]  # convert 0 to None
        else:
            values = [None] * len(weeks)  # if source missing, fill with nulls
        series.append({
            "name": source,
            "type": "line",
            "data": values,
            "smooth": True,
            "connectNulls": False  # keeps breaks where data is missing
        })

    # Check if all values across all series are zero
    all_zero = all(
        sum(s["data"]) == 0 for s in series
    )
    if all_zero:
        continue

    chart_options = {
        "title": {
            "text": f"{country} - Weekly Box Count",
            "textStyle": {"color": "#ffffff"}
        },
        "tooltip": {"trigger": "axis"},
        "legend": {
            "data": ["EDW", "FACT", "ANZ"],
            "textStyle": {"color": "#ffffff"}
        },
        "xAxis": {
            "type": "category",
            "data": weeks,
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
        "series": series,
        "grid": {"left": "3%", "right": "4%", "bottom": "15%", "containLabel": True}
    }

    st_echarts(options=chart_options, height="400px")


formatted_df = weekly_df.copy()

# Apply formatting to all numeric columns
for col in formatted_df.columns:
    if pd.api.types.is_numeric_dtype(formatted_df[col]):
        formatted_df[col] = formatted_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")

with st.expander("🔍 Show summary with all box counts"):
    st.dataframe(formatted_df, use_container_width=True)