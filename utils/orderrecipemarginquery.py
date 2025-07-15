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

def run_order_recipe_margin_raw(hellofresh_week_option,entity_option):
    query = f"""
          SELECT 
          bob_entity_code, hellofresh_week, composite_order_id, order_item_type, order_line_items_id, primary_tag, product_type, box_size,serves, number_of_recipes, kit_count,box_count, total_gross_revenue_excl_sales_tax, shipping_revenue_excl_tax, core_gross_revenue_excl_sales_tax, non_core_gross_revenue_excl_sales_tax, total_direct_costs, total_net_revenue_excl_sales_tax, net_p1c_margin
          FROM anz_finance_stakeholders.anz_orders_recipes
          WHERE hellofresh_week = '{hellofresh_week_option}' AND bob_entity_code= '{entity_option}'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

