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

def run_box_count_raw():
    query = f"""
          SELECT 
          source, hellofresh_week, country, recipe_family, box_plan, number_of_recipes, box_size, dc, kit_count, box_count
          FROM anz_finance_app.anz_orders_box_count
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

def run_kit_count_raw():
    query = f"""
        SELECT 
        hellofresh_week,
        country,
        recipe_index as slot,
        recipe_family,
        box_plan,
        number_of_recipes,
        box_size,
        dc,
        kit_count
        FROM anz_finance_app.anz_orders_slot_details
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

