import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from openai import OpenAI
import numpy as np

from utils.orderrecipemarginquery import (
    run_order_recipe_margin_raw,
    run_incremental_revenue_raw
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
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Style ---
st.markdown("""
    <style>
        [data-testid="stSidebarNav"] { display: none !important; }
        .metric-box {
            background-color: #f0f2f6;
            padding: 1.2rem;
            border-radius: 10px;
            text-align: center;
            margin-bottom: 1rem;
            box-shadow: 0 0 10px rgba(0,0,0,0.05);
        }
        .metric-label {
            font-size: 1rem;
            color: #666;
        }
        .metric-value {
            font-size: 1.8rem;
            font-weight: bold;
            color: #2c3e50;
        }
    </style>
""", unsafe_allow_html=True)



@st.cache_data(show_spinner="Loading Box Order Recipe data...", persist=False)
def load_order_recipe_margin_raw_data(hellofresh_week_option,entity_option):
    return run_order_recipe_margin_raw(hellofresh_week_option,entity_option)


@st.cache_data(show_spinner="Loading Incremental revenue data...", persist=False)
def load_incremental_revenue_raw_data():
    return run_incremental_revenue_raw()



@st.cache_data(show_spinner=False)
def get_hellofresh_weeks():
    df = fetch_hellofresh_weeks()
    return df['hellofresh_week'].tolist()


st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)


# if not st.user.is_logged_in:
#     st.switch_page("home.py")



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
    entity_option = st.selectbox(
        "Entity", 
        options=["AU","AO","NZ"]
    )

# Load raw data (cached per version)
df_raw = load_order_recipe_margin_raw_data(hellofresh_week_option,entity_option)




st.header(f"[{entity_option}] {hellofresh_week_option} Box Contribution Margin")

st.markdown(
    """
    <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
    """,
    unsafe_allow_html=True
)





# --- Metrics Calculation ---
total_box_count = int(df_raw["box_count"].sum())
total_gross_revenue = df_raw["total_gross_revenue_excl_sales_tax"].sum()
total_kit_count = int(df_raw["kit_count"].sum())
total_cogs = df_raw["total_direct_costs"].sum()

# Prevent divide-by-zero
total_aov = total_gross_revenue / total_box_count if total_box_count else 0
rev_per_serve = total_gross_revenue / total_kit_count if total_kit_count else 0
gross_margin_p1 = (1 - (total_cogs / total_gross_revenue)) * 100 if total_gross_revenue else 0
net_margin_p1 = (1 - (total_cogs / df_raw["total_net_revenue_excl_sales_tax"].sum())) * 100 if df_raw["total_net_revenue_excl_sales_tax"].sum() else 0

# --- Display Summary Cards ---
col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

def format_number_auto(value):
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.2f}K"
    else:
        return f"${value:,.0f}"

with col1:
    st.markdown(f'<div class="metric-box"><div class="metric-label">📦 Box Count</div><div class="metric-value">{total_box_count:,}</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-box"><div class="metric-label">💰 Total Gross Revenue</div><div class="metric-value">{format_number_auto(total_gross_revenue)}</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-box"><div class="metric-label">AOV</div><div class="metric-value">${total_aov:,.2f}</div></div>', unsafe_allow_html=True)
with col4:
    st.markdown(f'<div class="metric-box"><div class="metric-label">ASP</div><div class="metric-value">${rev_per_serve:,.2f}</div></div>', unsafe_allow_html=True)
with col5:
    st.markdown(f'<div class="metric-box"><div class="metric-label">🧾 Total COGS</div><div class="metric-value">{format_number_auto(total_cogs)}</div></div>', unsafe_allow_html=True)
with col6:
    st.markdown(f'<div class="metric-box"><div class="metric-label">PC1 Margin (Gross)</div><div class="metric-value">{gross_margin_p1:,.2f}%</div></div>', unsafe_allow_html=True)
with col7:
    st.markdown(f'<div class="metric-box"><div class="metric-label">PC1 Margin (Net)</div><div class="metric-value">{net_margin_p1:,.2f}%</div></div>', unsafe_allow_html=True)


# --- Summary by Product Type ---
st.header("Summary By Product Type")

summary_df = (
    df_raw
    .groupby("product_type", as_index=False)
    .agg(
        gross_revenue=("total_gross_revenue_excl_sales_tax", "sum"),
        direct_costs=("total_direct_costs", "sum"),
        net_revenue=("total_net_revenue_excl_sales_tax", "sum"),
        total_serves=("serves", "sum"),
        total_kits=("kit_count", "sum"),
    )
)

# Safe calculations
summary_df["revenue_per_box"] = np.where(total_box_count == 0, 0, summary_df["gross_revenue"] / total_box_count)
summary_df["revenue_per_serve"] = np.where(
    (summary_df["product_type"] == "Addon") | (summary_df["total_serves"] == 0),
    0,
    summary_df["gross_revenue"] / summary_df["total_serves"]
)
summary_df["asp"] = np.where(summary_df["total_kits"] == 0, 0, summary_df["gross_revenue"] / summary_df["total_kits"])
summary_df["cpk"] = np.where(summary_df["total_kits"] == 0, 0, summary_df["direct_costs"] / summary_df["total_kits"])
summary_df["cps"] = summary_df.apply(
    lambda row: 0 if (row["product_type"] == "Addon" or row["total_serves"] == 0)
    else row["direct_costs"] / row["total_serves"],
    axis=1
)
summary_df["gross_margin_percent"] = np.where(summary_df["gross_revenue"] == 0, 0, (1 - summary_df["direct_costs"] / summary_df["gross_revenue"]) * 100)
summary_df["net_margin_percent"] = np.where(summary_df["net_revenue"] == 0, 0, (1 - summary_df["direct_costs"] / summary_df["net_revenue"]) * 100)

# Rename and display
summary_df = summary_df[[
    "product_type", "gross_revenue", "revenue_per_box", "asp", "revenue_per_serve",
    "cpk", "cps", "gross_margin_percent", "net_margin_percent"
]]

summary_df.columns = [
    "Product Type", "Gross Revenue", "AOV", "ASP", "Rev per serve",
    "Cost per Kit", "Cost per Serve", "Gross Margin (%)", "Net Margin (%)"
]

summary_df = summary_df.sort_values(by="Gross Revenue", ascending=False)
summary_df.index = [''] * len(summary_df)

st.dataframe(summary_df.style.format({
    "Gross Revenue": "${:,.0f}", "AOV": "${:,.2f}", "ASP": "${:,.2f}",
    "Rev per serve": "${:,.2f}", "Cost per Kit": "${:,.2f}", "Cost per Serve": "${:,.2f}",
    "Gross Margin (%)": "{:.2f}%", "Net Margin (%)": "{:.2f}%"
}), use_container_width=True)


# --- Summary by Primary Tag ---
st.header("Summary by Primary Tag")

combined_summary_df = (
    df_raw
    .groupby(["primary_tag", "product_type"], as_index=False)
    .agg(
        gross_revenue=("total_gross_revenue_excl_sales_tax", "sum"),
        direct_costs=("total_direct_costs", "sum"),
        net_revenue=("total_net_revenue_excl_sales_tax", "sum"),
        total_serves=("serves", "sum"),
        total_kits=("kit_count", "sum")
    )
)

combined_summary_df["revenue_per_box"] = np.where(total_box_count == 0, 0, combined_summary_df["gross_revenue"] / total_box_count)
combined_summary_df["revenue_per_serve"] = np.where(
    (combined_summary_df["product_type"] == "Addon") | (combined_summary_df["total_serves"] == 0),
    0,
    combined_summary_df["gross_revenue"] / combined_summary_df["total_serves"]
)
combined_summary_df["asp"] = np.where(combined_summary_df["total_kits"] == 0, 0, combined_summary_df["gross_revenue"] / combined_summary_df["total_kits"])
combined_summary_df["cpk"] = np.where(combined_summary_df["total_kits"] == 0, 0, combined_summary_df["direct_costs"] / combined_summary_df["total_kits"])

combined_summary_df["cps"] = combined_summary_df.apply(
    lambda row: 0 if (row["product_type"] == "Addon" or row["total_serves"] == 0)
    else row["direct_costs"] / row["total_serves"],
    axis=1
)
combined_summary_df["gross_margin_percent"] = np.where(combined_summary_df["gross_revenue"] == 0, 0, (1 - combined_summary_df["direct_costs"] / combined_summary_df["gross_revenue"]) * 100)
combined_summary_df["net_margin_percent"] = np.where(combined_summary_df["net_revenue"] == 0, 0, (1 - combined_summary_df["direct_costs"] / combined_summary_df["net_revenue"]) * 100)

combined_summary_df = combined_summary_df[[
    "primary_tag", "product_type", "gross_revenue", "revenue_per_box", "asp", "revenue_per_serve",
    "cpk", "cps", "gross_margin_percent", "net_margin_percent"
]]

combined_summary_df.columns = [
    "Primary Tag", "Product Type", "Gross Revenue", "AOV", "ASP", "Rev per serve",
    "Cost per Kit", "Cost per Serve", "Gross Margin (%)", "Net Margin (%)"
]

combined_summary_df = combined_summary_df.sort_values(by="Gross Revenue", ascending=False)
combined_summary_df.index = [''] * len(combined_summary_df)

st.dataframe(combined_summary_df.style.format({
    "Gross Revenue": "${:,.0f}", "AOV": "${:,.2f}", "ASP": "${:,.2f}",
    "Rev per serve": "${:,.2f}", "Cost per Kit": "${:,.2f}", "Cost per Serve": "${:,.2f}",
    "Gross Margin (%)": "{:.2f}%", "Net Margin (%)": "{:.2f}%"
}), use_container_width=True)



# --- Summary by Primary Tag ---
st.header("Raw Data...")
# --- Optional: Show Raw Table ---
with st.expander("🔍 View Filtered Raw Data"):
    st.dataframe(df_raw, use_container_width=True)


st.header("Incremental..")
df_incremental = load_incremental_revenue_raw_data()

# Pivot table: weeks as columns
if not df_incremental.empty:
    pivot_df = df_incremental.pivot_table(
        index=["country", "product_type", "recipe_slot","box_size"],
        columns="hellofresh_week",
        values="incremental_rev",
        aggfunc="sum"
    ).reset_index()

    # Optional: flatten columns and round values
    pivot_df.columns.name = None
    pivot_df.iloc[:, 3:] = pivot_df.iloc[:, 3:].round(2)

    st.dataframe(pivot_df, use_container_width=True)
else:
    st.info("No incremental revenue data available.")