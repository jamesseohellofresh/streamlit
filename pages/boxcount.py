import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from openai import OpenAI
import numpy as np
from pandas.api.types import CategoricalDtype
from streamlit_echarts import st_echarts
from datetime import datetime, timedelta

from utils.boxcountquery import (
    run_box_count_raw,
    run_kit_count_raw
)

from utils.commonquery import (
    fetch_hellofresh_weeks,
    blank_repeats,
    format_number_auto
)

# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance Portal",
    page_icon=":bulb:",
    layout="wide",
    initial_sidebar_state="expanded"
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

# --- Cached Data Loaders ---
@st.cache_data(show_spinner="Loading Box Count data...", persist=True)
def load_box_count_raw_data():
    return run_box_count_raw()

@st.cache_data(show_spinner="Loading Kit Count data...", persist=True)
def load_kit_count_raw_data():
    return run_kit_count_raw()

@st.cache_data(show_spinner=False)
def get_hellofresh_weeks():
    df = fetch_hellofresh_weeks()
    return df['hellofresh_week'].tolist()


# --- Auth Check ---
# if not st.user.is_logged_in:
#     st.switch_page("home.py")


# --- Sidebar ---
if st.sidebar.button("🏠"):
    st.switch_page("home.py")

st.sidebar.markdown("<div style='border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;'></div>", unsafe_allow_html=True)

view_option = st.sidebar.radio("Select Data Type", ["Box Count", "Kit Count"])

st.sidebar.markdown("<div style='border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;'></div>", unsafe_allow_html=True)

hellofresh_weeks = get_hellofresh_weeks()
hellofresh_week_option = st.sidebar.selectbox("Hello Fresh Week", options=hellofresh_weeks)

# --- Main View Switch ---
if view_option == "Box Count":
    st.header(f"{hellofresh_week_option} Box Count")

    df_raw_all = load_box_count_raw_data()
    df_raw = df_raw_all[df_raw_all["hellofresh_week"] == hellofresh_week_option]

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

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.markdown(f"<div class='country-card'><div class='country-title'>Country</div><div class='country-value'>{row['country']}</div></div>", unsafe_allow_html=True)
        col2.markdown(f"<div class='country-card'><div class='country-title'>EDW</div><div class='country-value'>{edw:,}</div></div>", unsafe_allow_html=True)
        col3.markdown(f"<div class='country-card'><div class='country-title'>FACT</div><div class='country-value'>{fact:,} {fact_delta_html}</div></div>", unsafe_allow_html=True)
        col4.markdown(f"<div class='country-card'><div class='country-title'>ANZ</div><div class='country-value'>{anz:,} {anz_delta_html}</div></div>", unsafe_allow_html=True)

    # --- Weekly Trends ---
    weekly_df = (
        df_raw_all.groupby(["hellofresh_week", "country", "source"])["box_count"]
        .sum()
        .reset_index()
        .pivot_table(index="hellofresh_week", columns=["country", "source"], values="box_count", fill_value=0)
        .sort_index()
    )

    st.markdown("## 📈 Weekly Box Count Trends (EDW vs FACT vs ANZ)")
    cols = st.columns(3)

    for idx, country in enumerate(["AU", "AO", "NZ"]):
        weeks = weekly_df.index.tolist()
        series = []

        for source in ["EDW", "FACT", "ANZ"]:
            col_key = (country, source)
            if col_key in weekly_df.columns:
                values = weekly_df[col_key].tolist()
                values = [v if v > 0 else None for v in values]
            else:
                values = [None] * len(weeks)

            series.append({
                "name": source,
                "type": "line",
                "data": values,
                "smooth": True,
                "connectNulls": False
            })

        all_zero = all(not any(s["data"]) or sum(filter(None, s["data"])) == 0 for s in series)
        if all_zero:
            continue

        chart_options = {
            "title": {"text": f"{country} - Weekly Box Count", "textStyle": {"color": "#ffffff"}},
            "tooltip": {"trigger": "axis"},
            "legend": {"data": ["EDW", "FACT", "ANZ"], "bottom": 10, "textStyle": {"color": "#ffffff"}},
            "xAxis": {
                "type": "category",
                "data": [str(w) for w in weeks],
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
            "grid": {"top": 50, "left": "3%", "right": "4%", "bottom": 50, "containLabel": True}
        }

        with cols[idx % 3]:
            st_echarts(options=chart_options, height="400px")

    formatted_df = weekly_df.copy()
    for col in formatted_df.columns:
        if pd.api.types.is_numeric_dtype(formatted_df[col]):
            formatted_df[col] = formatted_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")

    with st.expander("🔍 Show summary with all box counts"):
        st.dataframe(formatted_df, use_container_width=True)

# --- Kit Count View ---
else:
    st.header(f"{hellofresh_week_option} Kit Count")
    df_kit = load_kit_count_raw_data()
    df_filtered = df_kit[df_kit["hellofresh_week"] == hellofresh_week_option]
    st.dataframe(df_filtered, use_container_width=True, height=650, hide_index=True)