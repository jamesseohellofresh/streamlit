import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from openai import OpenAI

from utils.budgetrecipecompositionquery import (
    run_recipe_composition_raw
)

from utils.commonquery import (
    fetch_hellofresh_weeks,
    blank_repeats
)

from datetime import datetime,timedelta
##from streamlit_autorefresh import st_autorefresh

# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance Portal",
    page_icon=":bulb:",
    layout="wide"
)

@st.cache_data(show_spinner="Loading Recipe Composition data...", persist=True)
def load_raw_data(version):
    return run_recipe_composition_raw(version)


# Load key from secrets
client = OpenAI(api_key=st.secrets["openai"]["OPENAI_API_KEY"])


st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)


if not st.user.is_logged_in:
    st.switch_page("home.py")



# --- UI ---


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
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

with st.sidebar:
    version_option = st.selectbox(
        "Version", 
        options=["H2 2025"]
    )
    period_option = st.selectbox(
        "View", 
        options=["Weekly","Monthly"]
    )
    entity_option = st.selectbox(
        "Entity", 
        options=["AU","AO","NZ"]
    )
    dc_option = st.selectbox(
        "DC", 
        options=["ALL","Sydney","Perth","Auckland"]
    )
    sku_option = st.selectbox(
        "Summary Level", 
        options=["By Item","Category","Primary Tag"]
    )
    category_option = st.radio(
        "Select Category", 
        options=["ALL","PTN", "PHF", "PRO", "DAI", "BAK", "DRY", "SPI", "CON"]
    )


# Load raw data (cached per version)
df_raw = load_raw_data(version_option)

if dc_option != "ALL":
    df_filtered = df_raw[(df_raw["country"] == entity_option) & (df_raw["dc"] == dc_option)]
else:
    df_filtered = df_raw[df_raw["country"] == entity_option]


if category_option != "ALL":
    df_filtered = df_filtered[df_filtered["category"] == category_option]


# Determine index columns
if sku_option == "By Item":
    index_cols = ["country", "category", "sku_code", "sku_name"]
elif sku_option == "Primary Tag":
    index_cols = ["country", "primary_tag"]
else:
    index_cols = ["country", "category"]


# Determine pivot column (weekly or monthly)
pivot_col = "hellofresh_week" if period_option == "Weekly" else "hellofresh_month"
with st.spinner("⏳ Preparing data..."):
    # --- SKU Uptake ---
    uptake_df = df_filtered.pivot_table(
        index=index_cols,
        columns=pivot_col,
        values="sku_uptake",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # --- Total Cost ---
    cost_df = df_filtered.pivot_table(
        index=index_cols,
        columns=pivot_col,
        values="total_cost",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # --- Total Cost ---
    cost_cols = [col for col in cost_df.columns if col not in index_cols]
    ##cost_df = cost_df.rename(columns={col: f"{col}_total_cost" for col in cost_cols})
    cost_df["__total_cost"] = cost_df[[col for col in cost_df.columns if col.startswith("202")]].sum(axis=1)
    cost_sorted = cost_df.sort_values("__total_cost", ascending=False).drop(columns="__total_cost")
    st.header("Recipe Composition By Total Costs")
    st.dataframe(cost_sorted.style.format({col: "{:,.0f}" for col in cost_sorted.columns if col.startswith("202")}), use_container_width=True, hide_index=True, height=550)

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    # --- SKU Uptake ---
    uptake_cols = [col for col in uptake_df.columns if col not in index_cols]
    # uptake_df = uptake_df.rename(columns={col: f"{col}_uptake" for col in uptake_cols})
    uptake_df["__total_cost"] = cost_df["__total_cost"]
    uptake_sorted = uptake_df.sort_values("__total_cost", ascending=False).drop(columns="__total_cost")
    st.header("Recipe Composition By SKU Uptake")
    st.dataframe(uptake_sorted.style.format({col: "{:,.0f}" for col in uptake_sorted.columns if col.startswith("202")}), use_container_width=True, hide_index=True, height=550)


    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    # --- Price ---
    if sku_option == "By Item":
        price_df = df_filtered.pivot_table(
            index=index_cols,
            columns=pivot_col,
            values="avg_price",
            aggfunc="mean",
            fill_value=0
        ).reset_index()
    elif sku_option == "Primary Tag":
        # Compute weighted price = avg_price * sku_uptake
        
        price_df = df_filtered.pivot_table(
            index=index_cols,
            columns=pivot_col,
            values="cpk",
            aggfunc="sum",  # this line avoids double-mean issue
            fill_value=0
        ).reset_index()
    else:
        # Group and compute avg_price = sum(total_cost) / sum(sku_uptake)
        grouped = df_filtered.groupby(index_cols + [pivot_col]).agg({
            "total_cost": "sum",
            "sku_uptake": "sum"
        }).reset_index()
        grouped["avg_price"] = grouped["total_cost"] / grouped["sku_uptake"]
        
        price_df = grouped.pivot_table(
            index=index_cols,
            columns=pivot_col,
            values="avg_price",
            aggfunc="sum",  # this line avoids double-mean issue
            fill_value=0
        ).reset_index()

    price_cols = [col for col in price_df.columns if col not in index_cols]
    #price_df = price_df.rename(columns={col: f"{col}_price" for col in price_cols})
    price_df["__total_cost"] = cost_df["__total_cost"]
    price_sorted = price_df.sort_values("__total_cost", ascending=False).drop(columns="__total_cost")
    st.header("Recipe Composition By SKU Price")
    st.dataframe(price_sorted.style.format({col: "{:,.2f}" for col in price_sorted.columns if col.startswith("202")}), use_container_width=True, hide_index=True, height=550)



    # sample = df_filtered.to_csv(index=False)

    # user_question = st.text_area("Ask something about the filtered data:")

    # if st.button("Ask AI") and user_question:
    #     with st.spinner("Thinking..."):
    #         response = client.chat.completions.create(
    #             model="gpt-4.1-nano",
    #             messages=[
    #                 {
    #                     "role": "system",
    #                     "content": "You are a data analyst for HelloFresh. Analyze recipe composition data and answer the user's question based on provided CSV."
    #                 },
    #                 {
    #                     "role": "user",
    #                     "content": f"Here is the CSV data:\n\n{sample}\n\nNow answer:\n{user_question}"
    #                 }
    #             ]
    #         )
    #         st.subheader("🤖 GPT Response")
    #         st.write(response.choices[0].message.content)