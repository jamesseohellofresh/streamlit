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

def run_recipe_composition_raw(version):
    query = f"""
          SELECT 
            t.hellofresh_week,
            CONCAT(CAST(hellofresh_year AS STRING), '-M', LPAD(CAST(hellofresh_month AS STRING), 2, '0'))  as hellofresh_month,
            t.country,
            left(t.sku_code,3) as category,
            t.sku_code,
            t.sku_name,
            t.dc,
            t.primary_tag,
            SUM(coalesce(t.sku_uptake,0) *coalesce(k.total_kits,0)) as sku_uptake,
            SUM(coalesce(t.sku_uptake,0) *coalesce(k.total_kits,0)* coalesce(t.static_price,0)) as total_cost,
            SUM(coalesce(t.sku_uptake,0) *coalesce(k.total_kits,0) * coalesce(t.static_price,0))/ SUM(coalesce(t.sku_uptake,0) *coalesce(k.total_kits,0)) as avg_price,
            SUM(coalesce(t.cpk,0)) as cpk
            FROM anz_finance_stakeholders.anz_budget_recipe_composition_v3 t
            LEFT JOIN  
            (
                SELECT 
                delivery_week_name as hellofresh_week,
                recipe_type as recipe_type,
                country,
                CASE WHEN city = 'Auckland' THEN 'NZ' ELSE city end as dc,
                recipe_size as box_size,
                SUM(kits) as total_kits,
                sum(box_count) as total_box_count
                FROM anz_finance_stakeholders.anz_budget_kit_counts
                WHERE version = 'H2 2025'
                GROUP BY delivery_week_name, recipe_size, recipe_type, country, city
            ) k ON t.hellofresh_week = k.hellofresh_week AND t.country = k.country AND t.box_size = k.box_size AND t.primary_tag = k.recipe_type AND t.dc = CASE WHEN k.dc = 'NZ' THEN 'Auckland' ELSE k.dc end
            LEFT JOIN dimensions.date_dimension d ON t.hellofresh_week = d.hellofresh_week
            WHERE t.version ='H2 2025'
            AND coalesce(t.sku_uptake,0) > 0
            GROUP BY
            1,2,3,4,5,6,7,8
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

