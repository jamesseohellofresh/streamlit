import streamlit as st
import pandas as pd
from pyspark.sql import functions as F
from openai import OpenAI
import numpy as np
from pandas.api.types import CategoricalDtype
from streamlit_echarts import st_echarts
from utils.query import (
    run_kraken_raw_data,
    run_kraken_trend_total_cost,
    run_kraken_trend_supplier_split_error,
    run_kit_count_to_production_data,
    run_kraken_null_price_error,
    run_kraken_null_price_error_trends,
    run_kraken_cpk,
    run_kraken_slot_details,
    run_kraken_cpk_primary_tag,
    run_kraken_cpk_product_type,
    run_kraken_slot_details_primary_tag
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
        
    </style>
""", unsafe_allow_html=True)



@st.cache_data(show_spinner="Loading Kraken Ops data...", persist=False)
def load_raw_data(week, entity, version):
    return run_kraken_raw_data(week, entity, version)



@st.cache_data(show_spinner="Loading Kraken Ops Trending data...", persist=True)
def load_trend_data():
    return run_kraken_trend_total_cost()


@st.cache_data(show_spinner="Loading Supplier Split Trending data...", persist=True)
def load_supplier_error_data():
    return run_kraken_trend_supplier_split_error()

@st.cache_data(show_spinner="Loading Kit Count data...", persist=False)
def load_kit_count_to_production_data(week, entity):
    return run_kit_count_to_production_data(week,entity)


@st.cache_data(show_spinner="Loading Null Price Error data...", persist=False)
def load_null_price_data(version, week):
    return run_kraken_null_price_error(version, week)


@st.cache_data(show_spinner="Loading Null Price Error Trends data...", persist=False)
def load_null_price_data_trend(version):
    return run_kraken_null_price_error_trends(version)


@st.cache_data(show_spinner="Loading CPK data...", persist=False)
def load_cpk_by_slot(week, entity):
    return run_kraken_cpk(week, entity)

@st.cache_data(show_spinner="Loading CPK By Primary Tag data...", persist=False)
def load_cpk_by_primary_tag(week, entity):
    return run_kraken_cpk_primary_tag(week, entity)

@st.cache_data(show_spinner="Loading Slot data...", persist=False)
def load_slot_details(week, entity, slot):
    return run_kraken_slot_details(week, entity, slot)


@st.cache_data(show_spinner="Loading Recipe For Primary Tag data...", persist=False)
def load_primary_tag_details(week, entity, tag):
    return run_kraken_slot_details_primary_tag(week, entity, tag)


@st.cache_data(show_spinner="Loading CPK By Product Type data...", persist=False)
def load_cpk_by_product_type(week, entity):
    return run_kraken_cpk_product_type(week, entity)

@st.cache_data(show_spinner=False)
def get_hellofresh_weeks():
    df = fetch_hellofresh_weeks()
    return df['hellofresh_week'].tolist()


def compute_mix(df, version):
    df_ver = df[df['version'] == version].copy()
    df_ver['total_kitcount'] = df_ver.groupby('slot')['kitcount'].transform('sum')
    df_ver[f'mix_{version}'] = df_ver['kitcount'] / df_ver['total_kitcount']
    return df_ver[['slot', 'recipe_size', f'mix_{version}']]


@st.fragment
def render_summary():
    df_cpk = load_cpk_by_slot(hellofresh_week_option, entity_option)
    st.header(f"[{entity_option}] {hellofresh_week_option} CPK By Slots")

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    if df_cpk.empty:
        st.warning("No CPK data available for the selected filters.")
        return df_cpk
    
    # 1️⃣ Get filter options
    slot_list = sorted(df_cpk["slot"].unique())
    primary_tag_list = sorted(tag for tag in df_cpk["primary_tag"].unique() if pd.notnull(tag))
    product_type_list = sorted(pt for pt in df_cpk["product_type"].unique() if pd.notnull(pt))

    # 2️⃣ UI - 3 Column Layout
    col1, col2, col3 = st.columns(3)

    with col1:
        selected_slots = st.multiselect("🎯 Filter Slots", options=slot_list, default=[])

    with col2:
        selected_tags = st.multiselect("🏷️ Filter Primary Tags", options=primary_tag_list, default=[])

    with col3:
        selected_product_types = st.multiselect("📂 Filter Product Types", options=product_type_list, default=[])

    # 3️⃣ Default to ALL if none selected
    if not selected_slots:
        selected_slots = slot_list
    if not selected_tags:
        selected_tags = primary_tag_list
    if not selected_product_types:
        selected_product_types = product_type_list
        
    # 4️⃣ Filter df_cpk before pivoting
    df_cpk = df_cpk[
        df_cpk["slot"].isin(selected_slots) &
        df_cpk["primary_tag"].isin(selected_tags) &
        df_cpk["product_type"].isin(selected_product_types)
    ]

    # Group multiple recipe titles per slot into one string
    title_map = df_cpk.groupby("slot")["recipe_title"].apply(lambda x: " | ".join(sorted(set(x)))).reset_index()
    df_cpk = df_cpk.drop(columns="recipe_title").drop_duplicates()
    df_cpk = df_cpk.merge(title_map, on="slot", how="left")

    # --- 1. Add product mix: recipe kitcount / total kitcount of that version ---
    total_kitcount_v2 = df_cpk[df_cpk['version'] == 'v2']['kitcount'].sum()
    total_kitcount_v3 = df_cpk[df_cpk['version'] == 'v3']['kitcount'].sum()

    df_cpk['mix_v2'] = np.where(
        df_cpk['version'] == 'v2',
        df_cpk['kitcount'] / total_kitcount_v2,
        np.nan
    )

    df_cpk['mix_v3'] = np.where(
        df_cpk['version'] == 'v3',
        df_cpk['kitcount'] / total_kitcount_v3,
        np.nan
    )

    # --- 2. Group recipe titles per slot ---
    title_map = df_cpk.groupby("slot")["recipe_title"].apply(lambda x: " | ".join(sorted(set(x)))).reset_index()
    df_cpk = df_cpk.drop(columns="recipe_title").drop_duplicates()
    df_cpk = df_cpk.merge(title_map, on="slot", how="left")

    # --- 3. Set index and column structure ---
    index_cols = ["slot","product_type", "recipe_title", "primary_tag"]
    column_levels = ["Metric", "Recipe Size", "Version"]

    # --- 4. Pivot each metric ---
    pivot_cpk = df_cpk.pivot_table(index=index_cols, columns=["recipe_size", "version"], values="cpk", aggfunc="sum", fill_value=0)
    pivot_kit = df_cpk.pivot_table(index=index_cols, columns=["recipe_size", "version"], values="kitcount", aggfunc="sum", fill_value=0)

    # For mix, filter valid rows only
    # Prepare mix_v2 pivot
    mix_v2_df = df_cpk[df_cpk["version"] == "v2"].copy()
    mix_v2_df = mix_v2_df.pivot_table(
        index=index_cols,
        columns="recipe_size",
        values="mix_v2",
        aggfunc="sum",
        fill_value=0
    )
    mix_v2_df.columns = pd.MultiIndex.from_tuples([("Mix", size, "v2") for size in mix_v2_df.columns], names=column_levels)

    # Prepare mix_v3 pivot
    mix_v3_df = df_cpk[df_cpk["version"] == "v3"].copy()
    mix_v3_df = mix_v3_df.pivot_table(
        index=index_cols,
        columns="recipe_size",
        values="mix_v3",
        aggfunc="sum",
        fill_value=0
    )
    mix_v3_df.columns = pd.MultiIndex.from_tuples([("Mix", size, "v3") for size in mix_v3_df.columns], names=column_levels)

    # Combine mix safely
    pivot_mix = pd.concat([mix_v2_df, mix_v3_df], axis=1)

    # --- 5. Add variance only to cpk and kitcount ---
    for size in sorted(df_cpk["recipe_size"].unique()):
        for metric, pivot_df in [("CPK", pivot_cpk), ("Kitcount", pivot_kit)]:
            try:
                v2 = pivot_df[(size, "v2")]
                v3 = pivot_df[(size, "v3")]
                pivot_df[(size, "variance")] = (v3 - v2) / v2.replace(0, np.nan)
            except KeyError:
                continue

    # --- 6. Rename all to 3-level columns ---
    pivot_cpk.columns = pd.MultiIndex.from_tuples([("CPK",) + col for col in pivot_cpk.columns], names=column_levels)
    pivot_kit.columns = pd.MultiIndex.from_tuples([("Kitcount",) + col for col in pivot_kit.columns], names=column_levels)

    # --- 7. Combine ---
    df_all = pd.concat([pivot_cpk, pivot_kit, pivot_mix], axis=1)
    df_all = df_all.sort_index(axis=1)
    df_all = df_all.reset_index()

    # 스타일 적용
    styled_df = df_all.style.format({
        col: "${:,.2f}" for col in df_all.columns if col[0] == "CPK" and col[2] in ("v2", "v3")
    } | {
        col: "{:,.0f}" for col in df_all.columns if col[0] == "Kitcount" and col[2] in ("v2", "v3")
    } | {
        col: "{:+.2%}" for col in df_all.columns if col[2] == "variance"
    } | {
        col: "{:.1%}" for col in df_all.columns if col[0] == "Mix"
    })

    # --- 9. Show ---
    st.dataframe(styled_df, use_container_width=True, height=600, hide_index=True)


    return df_cpk


@st.fragment
def render_slot_details(df_cpk):
    st.markdown("---")
    slot_list = sorted(df_cpk["slot"].unique())
    recipe_size_list = sorted(df_cpk["recipe_size"].unique())
    recipe_size_options = ["ALL"] + [str(size) for size in recipe_size_list]  # 👈 ensure string type for consistency

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        selected_slot = st.selectbox("🔎 View Slot Details", options=slot_list, index=0)
    with col2:
        selected_size =  st.selectbox("🔎 Recipe Size", options=recipe_size_options, index=0, key="filter_recipe_size")
    with col3:
        by_sku_group = st.toggle("show details...", value=False)

    if selected_slot:
        df_slot_detail = load_slot_details(hellofresh_week_option, entity_option, selected_slot)

        # ➕ Filter only if not "ALL"
        if selected_size != "ALL":
            df_slot_detail = df_slot_detail[df_slot_detail["recipe_size"] == selected_size]


        if not df_slot_detail.empty:
            df = df_slot_detail.copy()

            if by_sku_group:
                index = ["sku_code", "sku_name"]
                df["sku_unit_cost"] = df["forecast_total_cost"] / df["forecast_sku_quantity"]
            else:
                df["sku_group"] = df["sku_code"].str[:3]
                df_grouped = df.groupby("sku_group", as_index=False).agg({
                    "forecast_sku_quantity": lambda x: x[df["version"] == "v2"].sum(),
                    "forecast_total_cost": lambda x: x[df["version"] == "v2"].sum(),
                })
                # Recompute sku_unit_cost
                df["sku_group"] = df["sku_code"].str[:3]
                df = df.groupby(["sku_group", "version"], as_index=False).agg({
                    "forecast_sku_quantity": "sum",
                    "forecast_total_cost": "sum"
                })
                df["sku_unit_cost"] = df["forecast_total_cost"] / df["forecast_sku_quantity"]
                index = "sku_group"

            pivoted = df.pivot_table(
                index=index,
                columns="version",
                values=["forecast_sku_quantity", "sku_unit_cost", "forecast_total_cost"],
                aggfunc="sum",
                fill_value=0
            ).reset_index()

            pivoted[("forecast_sku_quantity", "variance %")] = (
                (pivoted[("forecast_sku_quantity", "v3")] - pivoted[("forecast_sku_quantity", "v2")]) /
                pivoted[("forecast_sku_quantity", "v2")].replace(0, np.nan)
            )
            pivoted[("sku_unit_cost", "variance %")] = (
                (pivoted[("sku_unit_cost", "v3")] - pivoted[("sku_unit_cost", "v2")]) /
                pivoted[("sku_unit_cost", "v2")].replace(0, np.nan)
            )
            pivoted[("forecast_total_cost", "variance %")] = (
                (pivoted[("forecast_total_cost", "v3")] - pivoted[("forecast_total_cost", "v2")]) /
                pivoted[("forecast_total_cost", "v2")].replace(0, np.nan)
            )

            pivoted.columns = [
                ("", "sku_group") if col == ("sku_group", "") else
                ("", "sku_code") if col == ("sku_code", "") else
                ("", "sku_name") if col == ("sku_name", "") else
                ("Qty", col[1]) if col[0] == "forecast_sku_quantity" else
                ("Cost", col[1]) if col[0] == "sku_unit_cost" else
                ("Total Cost", col[1]) if col[0] == "forecast_total_cost" else
                col
                for col in pivoted.columns
            ]

            cols = pivoted.columns.tolist()
            qty_order = [('Qty', 'v2'), ('Qty', 'v3'), ('Qty', 'variance %')] + [c for c in cols if c[0] == 'Qty' and c[1] not in ['v2', 'v3', 'variance %']]
            cost_cols = [c for c in cols if c[0] == 'Cost']
            total_cost_cols = [c for c in cols if c[0] == 'Total Cost']
            identity_cols = [c for c in cols if c[0] == '']
            new_order = identity_cols + qty_order + cost_cols + total_cost_cols
            pivoted = pivoted[new_order]
            pivoted.columns = pd.MultiIndex.from_tuples(pivoted.columns)

            def dollarwithdecimal(x): return f"${x:,.2f}"
            def dollar(x): return f"${x:,.0f}"

            def qty(x): return f"{x:,.0f}"
            def percent(x): return f"{x:+.1%}" if pd.notnull(x) else ""

            style_format = {
                ("Qty", "v2"): qty,
                ("Qty", "v3"): qty,
                ("Qty", "variance %"): percent,
                ("Cost", "v2"): dollarwithdecimal,
                ("Cost", "v3"): dollarwithdecimal,
                ("Cost", "variance %"): percent,
                ("Total Cost", "v2"): dollar,
                ("Total Cost", "v3"): dollar,
                ("Total Cost", "variance %"): percent,
            }

            recipe_title = df_cpk[df_cpk["slot"] == selected_slot]["recipe_title"].iloc[0]
            product_type = df_cpk[df_cpk["slot"] == selected_slot]["product_type"].iloc[0]
            primary_tag = df_cpk[df_cpk["slot"] == selected_slot]["primary_tag"].iloc[0]

            st.markdown(f"#### 📦 Slot Detail: `{selected_slot} [{primary_tag}] [{product_type}] {recipe_title}`")
            # --- Summary Totals ---
            sum_qty_v2 = pivoted[("Qty", "v2")].sum()
            sum_qty_v3 = pivoted[("Qty", "v3")].sum()
            sum_total_v2 = pivoted[("Total Cost", "v2")].sum()
            sum_total_v3 = pivoted[("Total Cost", "v3")].sum()

            avg_cost_v2 = sum_total_v2 / sum_qty_v2 if sum_qty_v2 else 0
            avg_cost_v3 = sum_total_v3 / sum_qty_v3 if sum_qty_v3 else 0

            qty_var_pct = (sum_qty_v3 - sum_qty_v2) / sum_qty_v2 * 100 if sum_qty_v2 else 0
            total_var_pct = (sum_total_v3 - sum_total_v2) / sum_total_v2 * 100 if sum_total_v2 else 0
            cost_var_pct = (avg_cost_v3 - avg_cost_v2) / avg_cost_v2 * 100 if avg_cost_v2 else 0
            
            st.markdown("---")

            colA, colB, sep1, colC, colD, sep2, colE, colF = st.columns([2, 2, 0.2, 2, 2, 0.2, 2, 2])

            with colA:
                st.metric("🧮 Total Qty v2", f"{sum_qty_v2:,.0f}")
            with colB:
                st.metric("🧮 Total Qty v3", f"{sum_qty_v3:,.0f}", delta=f"{qty_var_pct:+.1f}%")
            with sep1:
                st.markdown("<div style='height: 100%; border-left: 1px solid #ccc;'></div>", unsafe_allow_html=True)

            with colC:
                st.metric("📦 Avg Cost v2", f"${avg_cost_v2:,.2f}")
            with colD:
                st.metric("📦 Avg Cost v3", f"${avg_cost_v3:,.2f}", delta=f"{cost_var_pct:+.1f}%")
            with sep2:
                st.markdown("<div style='height: 100%; border-left: 1px solid #ccc;'></div>", unsafe_allow_html=True)

            with colE:
                st.metric("💰 Total Cost v2", f"${sum_total_v2:,.0f}")
            with colF:
                st.metric("💰 Total Cost v3", f"${sum_total_v3:,.0f}", delta=f"{total_var_pct:+.1f}%")

            st.dataframe(
                pivoted.style.format(style_format),
                use_container_width=True,
                hide_index=True,
                height=500
            )
        else:
            st.info("No SKU details available for this slot.")


@st.fragment
def render_summary_cpk_primary_tag():
    df_cpk = load_cpk_by_primary_tag(hellofresh_week_option, entity_option)
    st.header(f"[{entity_option}] {hellofresh_week_option} CPK By Primary Tag")

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    if df_cpk.empty:
        st.warning("No data available for the selected filters.")
        return df_cpk

    primary_tag_list = sorted(tag for tag in df_cpk["primary_tag"].unique() if pd.notnull(tag))
    product_type_list = sorted(pt for pt in df_cpk["product_type"].unique() if pd.notnull(pt))

    # 2️⃣ UI - 3 Column Layout
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_tags = st.multiselect("🏷️ Filter Primary Tags", options=primary_tag_list, default=[], key="filter_primary_tag")

    with col2:
        selected_product_types = st.multiselect("📂 Filter Product Types", options=product_type_list, default=[], key="filter_product_type")
        
    if not selected_tags:
        selected_tags = primary_tag_list
    if not selected_product_types:
        selected_product_types = product_type_list
        
    # 4️⃣ Filter df_cpk before pivoting
    df_cpk = df_cpk[
        df_cpk["primary_tag"].isin(selected_tags) &
        df_cpk["product_type"].isin(selected_product_types)
    ]

    # Normalize version casing
    df_cpk["version"] = df_cpk["version"].str.lower()

    # --- 1. Add product mix: recipe kitcount / total kitcount of that version ---
    total_kitcount_v2 = df_cpk[df_cpk['version'] == 'v2']['kitcount'].sum()
    total_kitcount_v3 = df_cpk[df_cpk['version'] == 'v3']['kitcount'].sum()

    df_cpk['mix_v2'] = np.where(
        df_cpk['version'] == 'v2',
        df_cpk['kitcount'] / total_kitcount_v2,
        np.nan
    )

    df_cpk['mix_v3'] = np.where(
        df_cpk['version'] == 'v3',
        df_cpk['kitcount'] / total_kitcount_v3,
        np.nan
    )

    index_cols = ["primary_tag", "product_type"]
    column_levels = ["Metric", "Recipe Size", "Version"]

    # --- 2. Pivot main metrics ---
    pivot_cpk = df_cpk.pivot_table(index=index_cols, columns=["recipe_size", "version"], values="cpk", aggfunc="sum", fill_value=0)
    pivot_kit = df_cpk.pivot_table(index=index_cols, columns=["recipe_size", "version"], values="kitcount", aggfunc="sum", fill_value=0)

    # --- 3. Pivot mix ---
    mix_v2_df = df_cpk[df_cpk["version"] == "v2"].pivot_table(
        index=index_cols, columns="recipe_size", values="mix_v2", aggfunc="sum", fill_value=0
    )
    mix_v2_df.columns = pd.MultiIndex.from_tuples([("Mix", size, "v2") for size in mix_v2_df.columns], names=column_levels)

    mix_v3_df = df_cpk[df_cpk["version"] == "v3"].pivot_table(
        index=index_cols, columns="recipe_size", values="mix_v3", aggfunc="sum", fill_value=0
    )
    mix_v3_df.columns = pd.MultiIndex.from_tuples([("Mix", size, "v3") for size in mix_v3_df.columns], names=column_levels)

    pivot_mix = pd.concat([mix_v2_df, mix_v3_df], axis=1)

    # --- 4. Add variance columns ---
    for size in sorted(df_cpk["recipe_size"].unique()):
        for metric, pivot_df in [("CPK", pivot_cpk), ("Kitcount", pivot_kit)]:
            try:
                v2 = pivot_df[(size, "v2")]
                v3 = pivot_df[(size, "v3")]
                pivot_df[(size, "variance")] = (v3 - v2) / v2.replace(0, np.nan)
            except KeyError:
                continue

    # --- 5. Rename levels ---
    pivot_cpk.columns = pd.MultiIndex.from_tuples([("CPK",) + col for col in pivot_cpk.columns], names=column_levels)
    pivot_kit.columns = pd.MultiIndex.from_tuples([("Kitcount",) + col for col in pivot_kit.columns], names=column_levels)

    # --- 6. Combine ---
    df_all = pd.concat([pivot_cpk, pivot_kit, pivot_mix], axis=1)
    df_all = df_all.sort_index(axis=1).reset_index()

    # Sort by total v3 kitcount across all recipe sizes
    v3_kit_cols = [col for col in df_all.columns if col[0] == "Kitcount" and col[2] == "v3"]
    df_all["__sort_total_v3_kitcount"] = df_all[v3_kit_cols].sum(axis=1)
    df_all = df_all.sort_values("__sort_total_v3_kitcount", ascending=False).drop(columns="__sort_total_v3_kitcount")

    # --- 7. Format style ---
    styled_df = df_all.style.format({
        col: "${:,.2f}" for col in df_all.columns if col[0] == "CPK" and col[2] in ("v2", "v3")
    } | {
        col: "{:,.0f}" for col in df_all.columns if col[0] == "Kitcount" and col[2] in ("v2", "v3")
    } | {
        col: "{:+.2%}" for col in df_all.columns if col[2] == "variance"
    } | {
        col: "{:.1%}" for col in df_all.columns if col[0] == "Mix"
    })

    st.dataframe(styled_df, use_container_width=True, height=600,hide_index= True)

    return df_cpk

@st.fragment
def render_primary_tag_details(df_cpk):
    st.markdown("---")
    primary_tag_list = sorted(tag for tag in df_cpk["primary_tag"].unique() if isinstance(tag, str))
    recipe_size_list = sorted(df_cpk["recipe_size"].unique())
    recipe_size_options = ["ALL"] + [str(size) for size in recipe_size_list]  # 👈 ensure string type for consistency

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        selected_primary_tag = st.selectbox("🔎 View Primary Tag Details", options=primary_tag_list, index=0)
    with col2:
        selected_size =  st.selectbox("🔎 Recipe Size", options=recipe_size_options, index=0, key="filter_recipe_size_primary_tag")
    with col3:
        by_sku_group = st.toggle("show details...", value=False, key="toggle_primary_tag")

    if selected_primary_tag:
        df_primary_tag_detail = load_primary_tag_details(hellofresh_week_option, entity_option, selected_primary_tag)
        # ➕ Filter only if not "ALL"
        if selected_size != "ALL":
            df_primary_tag_detail = df_primary_tag_detail[df_primary_tag_detail["recipe_size"] == selected_size]


        if not df_primary_tag_detail.empty:
            df = df_primary_tag_detail.copy()

            if by_sku_group:
                index = ["sku_code", "sku_name"]
            else:
                df["sku_group"] = df["sku_code"].str[:3]
                df = df.groupby(["sku_group", "version"], as_index=False).agg({
                    "forecast_sku_quantity": "sum",
                    "forecast_total_cost": "sum"
                })
                df["sku_unit_cost"] = df["forecast_total_cost"] / df["forecast_sku_quantity"]
                index = "sku_group"

            pivoted = df.pivot_table(
                index=index,
                columns="version",
                values=["forecast_sku_quantity", "sku_unit_cost", "forecast_total_cost"],
                aggfunc="sum",
                fill_value=0
            ).reset_index()

            # Add variance %
            pivoted[("forecast_sku_quantity", "variance %")] = (
                (pivoted[("forecast_sku_quantity", "v3")] - pivoted[("forecast_sku_quantity", "v2")]) /
                pivoted[("forecast_sku_quantity", "v2")].replace(0, np.nan)
            )
            pivoted[("sku_unit_cost", "variance %")] = (
                (pivoted[("sku_unit_cost", "v3")] - pivoted[("sku_unit_cost", "v2")]) /
                pivoted[("sku_unit_cost", "v2")].replace(0, np.nan)
            )
            pivoted[("forecast_total_cost", "variance %")] = (
                (pivoted[("forecast_total_cost", "v3")] - pivoted[("forecast_total_cost", "v2")]) /
                pivoted[("forecast_total_cost", "v2")].replace(0, np.nan)
            )

            # Rename top-level headers
            pivoted.columns = [
                ("", "sku_group") if col == ("sku_group", "") else
                ("", "sku_code") if col == ("sku_code", "") else
                ("", "sku_name") if col == ("sku_name", "") else
                ("Qty", col[1]) if col[0] == "forecast_sku_quantity" else
                ("Cost", col[1]) if col[0] == "sku_unit_cost" else
                ("Total Cost", col[1]) if col[0] == "forecast_total_cost" else
                col
                for col in pivoted.columns
            ]

            # Reorder columns
            cols = pivoted.columns.tolist()
            qty_order = [('Qty', 'v2'), ('Qty', 'v3'), ('Qty', 'variance %')] + [c for c in cols if c[0] == 'Qty' and c[1] not in ['v2', 'v3', 'variance %']]
            cost_cols = [c for c in cols if c[0] == 'Cost']
            total_cost_cols = [c for c in cols if c[0] == 'Total Cost']
            identity_cols = [c for c in cols if c[0] == '']
            new_order = identity_cols + qty_order + cost_cols + total_cost_cols
            pivoted = pivoted[new_order]
            pivoted.columns = pd.MultiIndex.from_tuples(pivoted.columns)

            # Summary cards
            sum_qty_v2 = pivoted[("Qty", "v2")].sum()
            sum_qty_v3 = pivoted[("Qty", "v3")].sum()
            sum_total_v2 = pivoted[("Total Cost", "v2")].sum()
            sum_total_v3 = pivoted[("Total Cost", "v3")].sum()
            avg_cost_v2 = sum_total_v2 / sum_qty_v2 if sum_qty_v2 else 0
            avg_cost_v3 = sum_total_v3 / sum_qty_v3 if sum_qty_v3 else 0
            qty_var_pct = (sum_qty_v3 - sum_qty_v2) / sum_qty_v2 * 100 if sum_qty_v2 else 0
            total_var_pct = (sum_total_v3 - sum_total_v2) / sum_total_v2 * 100 if sum_total_v2 else 0
            cost_var_pct = (avg_cost_v3 - avg_cost_v2) / avg_cost_v2 * 100 if avg_cost_v2 else 0

            st.markdown(f"#### 📦 Primary Tag Detail: `{selected_primary_tag}`")
            st.markdown("---")

            colA, colB, sep1, colC, colD, sep2, colE, colF = st.columns([2, 2, 0.2, 2, 2, 0.2, 2, 2])
            with colA:
                st.metric("🧮 Total Qty v2", f"{sum_qty_v2:,.0f}")
            with colB:
                st.metric("🧮 Total Qty v3", f"{sum_qty_v3:,.0f}", delta=f"{qty_var_pct:+.1f}%")
            with sep1:
                st.markdown("<div style='height: 40px; border-left: 1px solid #ccc;'></div>", unsafe_allow_html=True)
            with colC:
                st.metric("📦 Avg Cost v2", f"${avg_cost_v2:,.2f}")
            with colD:
                st.metric("📦 Avg Cost v3", f"${avg_cost_v3:,.2f}", delta=f"{cost_var_pct:+.1f}%")
            with sep2:
                st.markdown("<div style='height: 40px; border-left: 1px solid #ccc;'></div>", unsafe_allow_html=True)
            with colE:
                st.metric("💰 Total Cost v2", f"${sum_total_v2:,.0f}")
            with colF:
                st.metric("💰 Total Cost v3", f"${sum_total_v3:,.0f}", delta=f"{total_var_pct:+.1f}%")

            # Format functions
            def dollar(x): return f"${x:,.2f}"
            def qty(x): return f"{x:,.0f}"
            def percent(x): return f"{x:+.1%}" if pd.notnull(x) else ""

            style_format = {
                ("Qty", "v2"): qty,
                ("Qty", "v3"): qty,
                ("Qty", "variance %"): percent,
                ("Cost", "v2"): dollar,
                ("Cost", "v3"): dollar,
                ("Cost", "variance %"): percent,
                ("Total Cost", "v2"): dollar,
                ("Total Cost", "v3"): dollar,
                ("Total Cost", "variance %"): percent,
            }

            st.dataframe(
                pivoted.style.format(style_format),
                use_container_width=True,
                hide_index=True,
                height=500
            )
        else:
            st.info("No SKU details available for this primary tag.")


@st.fragment
def render_summary_cpk_product_type():
    df_cpk = load_cpk_by_product_type(hellofresh_week_option, entity_option)
    st.header(f"[{entity_option}] {hellofresh_week_option} CPK By Product Type")

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    if df_cpk.empty:
        st.warning("No data available for the selected filters.")
        return df_cpk

    # Normalize version casing
    df_cpk["version"] = df_cpk["version"].str.lower()

    # --- 1. Add product mix: recipe kitcount / total kitcount of that version ---
    total_kitcount_v2 = df_cpk[df_cpk['version'] == 'v2']['kitcount'].sum()
    total_kitcount_v3 = df_cpk[df_cpk['version'] == 'v3']['kitcount'].sum()

    df_cpk['mix_v2'] = np.where(
        df_cpk['version'] == 'v2',
        df_cpk['kitcount'] / total_kitcount_v2,
        np.nan
    )

    df_cpk['mix_v3'] = np.where(
        df_cpk['version'] == 'v3',
        df_cpk['kitcount'] / total_kitcount_v3,
        np.nan
    )

    index_cols = ["product_type"]
    column_levels = ["Metric", "Recipe Size", "Version"]

    # --- 2. Pivot main metrics ---
    pivot_cpk = df_cpk.pivot_table(index=index_cols, columns=["recipe_size", "version"], values="cpk", aggfunc="sum", fill_value=0)
    pivot_kit = df_cpk.pivot_table(index=index_cols, columns=["recipe_size", "version"], values="kitcount", aggfunc="sum", fill_value=0)

    # --- 3. Pivot mix ---
    mix_v2_df = df_cpk[df_cpk["version"] == "v2"].pivot_table(
        index=index_cols, columns="recipe_size", values="mix_v2", aggfunc="sum", fill_value=0
    )
    mix_v2_df.columns = pd.MultiIndex.from_tuples([("Mix", size, "v2") for size in mix_v2_df.columns], names=column_levels)

    mix_v3_df = df_cpk[df_cpk["version"] == "v3"].pivot_table(
        index=index_cols, columns="recipe_size", values="mix_v3", aggfunc="sum", fill_value=0
    )
    mix_v3_df.columns = pd.MultiIndex.from_tuples([("Mix", size, "v3") for size in mix_v3_df.columns], names=column_levels)

    pivot_mix = pd.concat([mix_v2_df, mix_v3_df], axis=1)

    # --- 4. Add variance columns ---
    for size in sorted(df_cpk["recipe_size"].unique()):
        for metric, pivot_df in [("CPK", pivot_cpk), ("Kitcount", pivot_kit)]:
            try:
                v2 = pivot_df[(size, "v2")]
                v3 = pivot_df[(size, "v3")]
                pivot_df[(size, "variance")] = (v3 - v2) / v2.replace(0, np.nan)
            except KeyError:
                continue

    # --- 5. Rename levels ---
    pivot_cpk.columns = pd.MultiIndex.from_tuples([("CPK",) + col for col in pivot_cpk.columns], names=column_levels)
    pivot_kit.columns = pd.MultiIndex.from_tuples([("Kitcount",) + col for col in pivot_kit.columns], names=column_levels)

    # --- 6. Combine ---
    df_all = pd.concat([pivot_cpk, pivot_kit, pivot_mix], axis=1)
    df_all = df_all.sort_index(axis=1).reset_index()

    # Sort by total v3 kitcount across all recipe sizes
    v3_kit_cols = [col for col in df_all.columns if col[0] == "Kitcount" and col[2] == "v3"]
    df_all["__sort_total_v3_kitcount"] = df_all[v3_kit_cols].sum(axis=1)
    df_all = df_all.sort_values("__sort_total_v3_kitcount", ascending=False).drop(columns="__sort_total_v3_kitcount")

    # --- 7. Format style ---
    styled_df = df_all.style.format({
        col: "${:,.2f}" for col in df_all.columns if col[0] == "CPK" and col[2] in ("v2", "v3")
    } | {
        col: "{:,.0f}" for col in df_all.columns if col[0] == "Kitcount" and col[2] in ("v2", "v3")
    } | {
        col: "{:+.2%}" for col in df_all.columns if col[2] == "variance"
    } | {
        col: "{:.1%}" for col in df_all.columns if col[0] == "Mix"
    })

    st.dataframe(styled_df, use_container_width=True, height=250,hide_index= True)

    return df_cpk




st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)


# if not st.user.is_logged_in:
#     st.switch_page("home.py")



# --- UI ---
st.cache_data.clear()


# Sidebar top
with st.sidebar:
    if st.button(f"🏠︎"):
        st.switch_page("home.py")

    # Tab selection (vertical)
    selected_tab = st.radio(
        "",
        ["Executive Summary", "Reconciliation","Null Price Error","CPK"]
    )

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

# Fetch once and reuse across reruns
hellofresh_weeks = get_hellofresh_weeks()

with st.sidebar:
    if selected_tab == "Executive Summary":
        hellofresh_week_option = st.selectbox(
            "HelloFresh Week",
            options=hellofresh_weeks
        )
        version_option = st.selectbox(
            "Version",
            options=["v3", "v2"]
        )

    elif selected_tab == "Reconciliation":
        hellofresh_week_option = st.selectbox(
            "Select Week",
            options=hellofresh_weeks
        )
        entity_option = st.selectbox(
            "Country",
            options=["AU","AO","NZ"]
        )
        version_option = "v3"  # fixed or hidden for this tab
    elif selected_tab == "Null Price Error":
        hellofresh_week_option = st.selectbox(
            "HelloFresh Week",
            options=hellofresh_weeks
        )
        version_option = st.selectbox(
            "Version",
            options=["v3", "v2"]
        )
    elif selected_tab == "CPK":
        hellofresh_week_option = st.selectbox(
            "HelloFresh Week",
            options=hellofresh_weeks
        )
        entity_option = st.selectbox(
            "Country",
            options=["AU","AO","NZ"]
        )

if selected_tab == "Executive Summary":
    
    # Load raw data (cached per version)
    # df_raw_all = load_raw_data(hellofresh_week_option, version_option)
    df_trends = load_trend_data()
    df_supplier_errors = load_supplier_error_data()

    df_trend_version= df_trends[df_trends["version"] == version_option]
    df_supplier_errors_version= df_supplier_errors[df_supplier_errors["version"] == version_option]

    df_trend_weeke_version = df_trends[
        (df_trends["version"] == version_option) & 
        (df_trends["hellofresh_week"] == hellofresh_week_option)
    ]

    country_order = CategoricalDtype(["AU", "AO", "NZ"], ordered=True)
    df_trend_weeke_version["bob_entity_code"] = df_trend_weeke_version["bob_entity_code"].astype(country_order)

    # --- Summary: Total forecast_total_cost by bob_entity_code ---
    summary_df = (
        df_trend_weeke_version.groupby("bob_entity_code")["forecast_total_cost"]
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

    supplier_split_error_summary = (
        df_supplier_errors_version.groupby(["hellofresh_week", "bob_entity_code"])["count_error"]
        .sum()
        .reset_index()
        .pivot(index="hellofresh_week", columns="bob_entity_code", values="count_error")
        .fillna(0)
        .astype(int)
        .sort_index()
    )



    # formatted_df = df_raw_all.copy()

    # # Apply formatting to all numeric columns
    # for col in formatted_df.columns:
    #     if pd.api.types.is_numeric_dtype(formatted_df[col]):
    #         formatted_df[col] = formatted_df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")

    # with st.expander("🔍 Show details..."):
    #     st.dataframe(formatted_df, use_container_width=True)

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

    cols = st.columns(3)

    for idx, entity in enumerate(["AU", "AO", "NZ"]):
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
                "textStyle": {"color": "#ffffff"},
                "bottom" : 10,
            },
            "xAxis": {
                "type": "category",
                "data": [str(x) for x in trend_summary.index.tolist()],
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

        # Display in each column
        with cols[idx]:
            st_echarts(options=options, height="400px")



    # Check which entities actually exist in the data
    available_entities = [e for e in ["AU", "AO", "NZ"] if e in supplier_split_error_summary.columns and supplier_split_error_summary[e].sum() > 0]

    # Only show the section if at least one chart is available
    if available_entities:

        # Section heading
        st.markdown("""
            <div style="margin-top: 28px; margin-bottom: 38px;"></div>
        """, unsafe_allow_html=True)

        st.markdown("### 📈 Supplier Split Errors by Entity")

        st.markdown("""
            <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """, unsafe_allow_html=True)

        # Create 3 columns for charts
        cols = st.columns(3)

        # Loop and plot only available entity charts
        for idx, entity in enumerate(available_entities):
            values = [
                val if val > 0 else None
                for val in supplier_split_error_summary[entity].tolist()
            ]

            options = {
                "title": {
                    "text": f"{entity} - Supplier Split Error",
                    "textStyle": {"color": "#ffffff"}
                },
                "tooltip": {"trigger": "axis"},
                "legend": {
                    "data": [entity],
                    "textStyle": {"color": "#ffffff"},
                    "bottom" : 10
                },
                "xAxis": {
                    "type": "category",
                    "data": [str(x) for x in supplier_split_error_summary.index.tolist()],
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

            with cols[idx % 3]:  # To handle fewer than 3 entities
                st_echarts(options=options, height="400px")


elif selected_tab == "Reconciliation":
    df_raw_all = load_raw_data(hellofresh_week_option, entity_option, version_option)
    df_kit_count = load_kit_count_to_production_data(hellofresh_week_option,entity_option)
    st.header(f"[{version_option.upper()}] {hellofresh_week_option} Kraken Ops Reconciliation")
    st.dataframe(df_raw_all, use_container_width=True, height=700)
    st.header(f"[{version_option.upper()}] {hellofresh_week_option} Kit Count Transformed To Production")
    # Pivot to make `recipe_size` columns
    df_pivoted = df_kit_count.pivot_table(
        index=["country", "dc", "recipe_index", "recipe_family", "recipe_type"],
        columns="recipe_size",
        values="kit_count",
        aggfunc="sum",
        fill_value=0
    ).reset_index()


    #Optional: Rename columns for clarity
    df_pivoted.columns.name = None  # Remove multiindex column name
    df_pivoted = df_pivoted.rename(columns={
        2: "2P",
        3: "3P",
        4: "4P"
    })
    st.dataframe(df_pivoted, use_container_width=True, height=700)



elif selected_tab == "Null Price Error":
    df_raw_price_error = load_null_price_data(version_option, hellofresh_week_option)
    
    st.header(f"[{version_option.upper()}] {hellofresh_week_option}  Null Price Error")

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    if not df_raw_price_error.empty:
        # Set order of countries
        country_order = CategoricalDtype(["AU", "AO", "NZ"], ordered=True)
        df_raw_price_error["bob_entity_code"] = df_raw_price_error["bob_entity_code"].astype(country_order)

        # Group and format total_costs
        summary_df = (
            df_raw_price_error.groupby("bob_entity_code")["total_costs"]
            .sum()
            .reset_index()
        )
        summary_df["total_costs"] = summary_df["total_costs"].fillna(0).astype(int).map("{:,}".format)

        cols = st.columns(len(summary_df))
        for i, row in summary_df.iterrows():
            cols[i].markdown(f"""
                <div style="background-color:#f8f9fc;padding:1rem;border-radius:10px;
                            box-shadow:0 2px 4px rgba(0,0,0,0.06);text-align:center">
                    <div style="font-size:1.1rem;color:#888;">{row['bob_entity_code']}</div>
                    <div style="font-size:1.8rem;font-weight:600;color:#2c3e50;">${row['total_costs']}</div>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No data available for the selected version and week.")


    st.markdown(
        """
        <div style="margin-top: 28px; margin-bottom: 38px;"></div>
        """,
        unsafe_allow_html=True
    )
    
    st.header(f"[{version_option.upper()}] Null Price Error Trends")

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )
    df_trend_price_error = load_null_price_data_trend(version_option)


    if not df_trend_price_error.empty:
        # Optional: sort week for better chart X-axis
        df_trend = df_trend_price_error.sort_values(["bob_entity_code", "hellofresh_week"])

        # Create 3 columns for AU, AO, NZ
        cols = st.columns(3)
        for i, entity in enumerate(["AU", "AO", "NZ"]):
            df_entity = df_trend[df_trend["bob_entity_code"] == entity]

            if not df_entity.empty:
                x_data = df_entity["hellofresh_week"].tolist()
                y_data = df_entity["total_costs"].fillna(0).astype(float).round(0).tolist()

                with cols[i]:
                    st.markdown(f"**{entity} Weekly Total Costs**")
                    st_echarts({
                        "xAxis": {"type": "category", "data": x_data},
                        "yAxis": {"type": "value"},
                        "series": [{
                            "data": y_data,
                            "type": "line",
                            "smooth": True,
                            "lineStyle": {"width": 2},
                        }],
                        "tooltip": {"trigger": "axis"},
                        "grid": {"left": 60, "right": 10, "top": 30, "bottom": 30}
                    }, height="260px")
            else:
                with cols[i]:
                    st.markdown(f"**{entity} Weekly Total Costs**")
                    st.info("No data available.")


    st.markdown(
        """
        <div style="margin-top: 28px; margin-bottom: 38px;"></div>
        """,
        unsafe_allow_html=True
    )

    st.header(f"[{version_option.upper()}] {hellofresh_week_option} Null Price Error Raw Data")

    st.markdown(
        """
        <div style="border-top: 2px solid #e74c3c; margin-top: 10px; margin-bottom: 18px;"></div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div style="margin-top: 28px; margin-bottom: 38px;"></div>
        """,
        unsafe_allow_html=True
    )
    # --- Detailed Data ---
    st.dataframe(df_raw_price_error, use_container_width=True, height=700)
    


elif selected_tab == "CPK":
    sub_tabs = st.tabs(["📦 By Slot", "🏷️ By Primary Tag", "📂 By Product Type"])

    with sub_tabs[0]:  # By Slot
        df_cpk = render_summary()
        render_slot_details(df_cpk)

    with sub_tabs[1]:  # By Primary Tag
        df_cpk_primary_tag = render_summary_cpk_primary_tag()
        render_primary_tag_details(df_cpk_primary_tag)
            
    with sub_tabs[2]:  # By Product Type
        df_cpk_product_type = render_summary_cpk_product_type()

            