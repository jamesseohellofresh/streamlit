import streamlit as st
from databricks import sql
import pandas as pd
import requests
import os
import time
from utils.db import (
    get_connection
)

DATABRICKS_HOST = st.secrets["databricks"]["host"]
HTTP_PATH = st.secrets["databricks"]["http_path"]
ACCESS_TOKEN = st.secrets["databricks"]["token"]

os.environ["DATABRICKS_HOST"] = f"https://{st.secrets["databricks"]["host"]}"
os.environ["DATABRICKS_TOKEN"] = st.secrets["databricks"]["token"]


# API endpoint
apiurl = f"{os.environ['DATABRICKS_HOST']}/api/2.1/jobs/runs/submit"

def run_kraken_raw_data(hellofresh_week_option, version):
    query = f"""
          SELECT 
          slot, recipe_title, recipe_size, octopus_recipe_id, sku_code, sku_name, sku_picks_per_recipe, sku_unit_cost,   supplier_name, supplier_code, supplier_split, supplier_country_of_origin, supplier_region, recipe_forecast_quantity, forecast_sku_quantity, forecast_total_cost, forecast_kitcount, dc, version, bob_entity_code, hellofresh_week
          FROM anz_finance_stakeholders.anz_kraken_operations_historical
          WHERE hellofresh_week = '{hellofresh_week_option}'
          AND version = '{version}'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

def run_kraken_trend_total_cost():
    query = f"""
          SELECT 
          version, bob_entity_code, hellofresh_week , SUM(forecast_total_cost) as forecast_total_cost
          FROM anz_finance_stakeholders.anz_kraken_operations_historical
          GROUP BY 1,2,3
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df
