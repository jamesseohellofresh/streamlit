import streamlit as st
from databricks import sql
import pandas as pd
import requests
import os
import time
from utils.db import (
    get_connection
)

DATABRICKS_HOST = st.secrets['databricks']['host']
HTTP_PATH = st.secrets['databricks']['http_path']
ACCESS_TOKEN = st.secrets['databricks']['token']

os.environ["DATABRICKS_HOST"] = f"https://{st.secrets['databricks']['host']}"
os.environ["DATABRICKS_TOKEN"] = st.secrets['databricks']['token']

# API endpoint
apiurl = f"{os.environ['DATABRICKS_HOST']}/api/2.1/jobs/runs/submit"

def run_sales_cogs_by_slot_raw(version, week):
    query = f"""
        select 
            M.hellofresh_week,
            M.version,
            M.country,
            M.recipe_slot,
            M.title,
            M.box_type,
            M.product_type,
            M.recipe_family,
            M.primary_tag,
            M.recipe_size,
            M.dc,
            M.sales_count_kit,
            M.cost_per_kit,
            M.cogs_per_kit,
            M.cost,
            M.cogs,
            M.box_count,
            M.core_sales,
            M.non_core_sales,
            M.residual_cost,
            M.residual_cogs,
            M.adj_cost_per_kit,
            M.adj_cogs_per_kit,
            M.adj_cost_per_box,
            M.adj_cogs_per_box
        FROM  anz_finance_app.sales_cogs_by_slots M
        WHERE M.version = '{version}' AND M.hellofresh_week ='{week}'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df


def run_recipes_raw_data(version, week):
    query = f"""
        
            SELECT 
            v.bob_entity_code,
            sku_code,
            slot,
            sku_name,
            recipe_size,
            v.sku_category,
            sku_picks_per_recipe,
            dc,
            supplier_code,
            supplier_name,
            supplier_split,
            forecast_sku_quantity,
            forecast_total_cost
            FROM  anz_operations.anz_kraken_operations_historical v
            WHERE v.hellofresh_week = '{week}'
            AND v.version = '{version}'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df


def fetch_hellofresh_weeks():
    conn = get_connection()
    query = """
        SELECT distinct hellofresh_week from hive_metastore.dimensions.date_dimension
        WHERE hellofresh_week between '2025-W01' ANd '2026-W52'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def process_sales_cogs_data(df, option, country):
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Filter by country
    df = df[df['country'] == country]
    if df.empty:
        return pd.DataFrame()

    if option == 'By Slot':
        df_grouped = (
            df.groupby(['recipe_slot', 'title', 'recipe_family','primary_tag'], as_index=False)
              .agg({
                  'sales_count_kit': 'sum',
                  'core_sales': 'sum',
                  'non_core_sales': 'sum',
                  'cogs': 'sum',
                  'residual_cogs': 'sum',
                  'box_count': 'sum',
                  'sales_count_kit': 'sum'
              })
        )

        # Total sales and adjusted cogs
        df_grouped['total_sales'] = df_grouped['core_sales'] + df_grouped['non_core_sales']
        df_grouped['total_cogs'] = df_grouped['cogs'] + df_grouped['residual_cogs']

        # Adjusted cogs ratio per sales
        df_grouped['cogs_per_sales'] = (
            df_grouped['total_cogs'] / df_grouped['total_sales']
        ).fillna(0)

        # Drop unneeded columns
        df_grouped.drop(columns=['core_sales', 'cogs', 'non_core_sales', 'residual_cogs'], inplace=True)

        return df_grouped
    elif option == "By Type":
        df_grouped = (
            df.groupby(['recipe_family'], as_index=False)
              .agg({
                  'sales_count_kit': 'sum',
                  'core_sales': 'sum',
                  'non_core_sales': 'sum',
                  'cogs': 'sum',
                  'residual_cogs': 'sum',
                  'box_count': 'sum',
                  'sales_count_kit': 'sum'
              })
        )

        # Total sales and adjusted cogs
        df_grouped['total_sales'] = df_grouped['core_sales'] + df_grouped['non_core_sales']
        df_grouped['total_cogs'] = df_grouped['cogs'] + df_grouped['residual_cogs']

        # Adjusted cogs ratio per sales
        df_grouped['cogs_per_sales'] = (
            df_grouped['total_cogs'] / df_grouped['total_sales']
        ).fillna(0)

        # Drop unneeded columns
        df_grouped.drop(columns=['core_sales', 'cogs', 'non_core_sales', 'residual_cogs'], inplace=True)

        return df_grouped
    
    elif option == "By Primary Tag":
        df_grouped = (
            df.groupby(['primary_tag'], as_index=False)
              .agg({
                  'sales_count_kit': 'sum',
                  'core_sales': 'sum',
                  'non_core_sales': 'sum',
                  'cogs': 'sum',
                  'residual_cogs': 'sum',
                  'box_count': 'sum',
                  'sales_count_kit': 'sum'
              })
        )

        # Total sales and adjusted cogs
        df_grouped['total_sales'] = df_grouped['core_sales'] + df_grouped['non_core_sales']
        df_grouped['total_cogs'] = df_grouped['cogs'] + df_grouped['residual_cogs']

        # Adjusted cogs ratio per sales
        df_grouped['cogs_per_sales'] = (
            df_grouped['total_cogs'] / df_grouped['total_sales']
        ).fillna(0)

        # Drop unneeded columns
        df_grouped.drop(columns=['core_sales', 'cogs', 'non_core_sales', 'residual_cogs'], inplace=True)

        return df_grouped
    



 
def process_recipe_data(df, option, country, slot):
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Filter by country
    df = df[(df['bob_entity_code'] == country) & (df['slot'].astype(int) == int(slot))]

    if df.empty:
        return pd.DataFrame()

    if option == 'By Slot':
        return df


def process_recipe_data_calc(df, option):
    df['weighted_quantity'] = df['forecast_sku_quantity'] * df['supplier_split']
    df['weighted_cost'] = df['forecast_total_cost'] * df['supplier_split']

    if option == "By Recipe":
        group_cols = ['sku_code','sku_name', 'sku_category']
    elif option == "By DC":
        group_cols = ['dc', 'sku_name', 'sku_category']
    elif option == "By Size":
        group_cols = ['recipe_size', 'sku_name', 'sku_category']
    elif option == "By DC & Size":
        group_cols = ['dc', 'recipe_size', 'sku_name', 'sku_category']
    else:
        return df  # Show raw if "All"

    grouped = (
        df.groupby(group_cols, as_index=False)
        .agg({
            'weighted_quantity': 'sum',
            'weighted_cost': 'sum'
        })
    )

    grouped['unit_cost'] = grouped['weighted_cost'] / grouped['weighted_quantity']
    return grouped