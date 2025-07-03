import streamlit as st
from databricks import sql
import pandas as pd
import requests
import os
import time
from utils.db import (
    get_connection
)


def fetch_hellofresh_weeks():
    conn = get_connection()
    query = """
        SELECT distinct hellofresh_week from hive_metastore.dimensions.date_dimension
        WHERE hellofresh_week between '2025-W01' ANd '2026-W52'
        ORDER BY hellofresh_week
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def blank_repeats(df, cols):
    df_copy = df.copy()
    for col in cols:
        last_val = None
        for i in df_copy.index:
            if df_copy.at[i, col] == last_val:
                df_copy.at[i, col] = ''
            else:
                last_val = df_copy.at[i, col]
    return df_copy