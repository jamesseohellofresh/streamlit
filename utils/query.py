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

def run_kraken_raw_data(hellofresh_week_option, entity, version):
    query = f"""
          SELECT 
          slot, recipe_title, recipe_size, octopus_recipe_id, sku_code, sku_name, 
          sku_picks_per_recipe, sku_unit_cost, supplier_name, supplier_code, supplier_split, 
          recipe_forecast_quantity, forecast_sku_quantity, forecast_total_cost, forecast_kitcount, dc, version, bob_entity_code, hellofresh_week
          FROM anz_finance_app.anz_kraken_operations_historical
          WHERE hellofresh_week = '{hellofresh_week_option}'
          AND version = '{version}'
          AND bob_entity_code = '{entity}'
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

def run_kit_count_to_production_data(hellofresh_week_option,entity):
    query = f"""

        SELECT 
        hellofresh_week,
        country,
        dc,
        recipe_index,
        recipe_family,
        CASE WHEN recipe_family like '%addons%' THEN 'addons' ELSE 'meals' END AS recipe_type,
        2 as recipe_size,
        sum(kit_count) as kit_count
        FROM anz_finance_app.anz_orders_slot_details 
        WHERE 
        hellofresh_week = '{hellofresh_week_option}'
        AND country = '{entity}'
        AND box_size in (1,2,5,6)
        GROUP BY 1,2,3,4,5,6

        UNION ALL

        SELECT 
        hellofresh_week,
        country,
        dc,
        recipe_index,
        recipe_family,
        CASE WHEN recipe_family like '%addons%' THEN 'addons' ELSE 'meals' END AS recipe_type,
        3 as recipe_size,
        sum(kit_count) as kit_count
        FROM anz_finance_app.anz_orders_slot_details 
        WHERE 
        hellofresh_week = '{hellofresh_week_option}'AND 
        country = '{entity}' AND
        country in ('AU','AO') AND
        box_size in (3,5)
        GROUP BY 1,2,3,4,5,6


        UNION ALL

        SELECT 
        hellofresh_week,
        country,
        dc,
        recipe_index,
        recipe_family,
        CASE WHEN recipe_family like '%addons%' THEN 'addons' ELSE 'meals' END AS recipe_type,
        4 as recipe_size,
        sum(kit_count) as kit_count
        FROM anz_finance_app.anz_orders_slot_details 
        WHERE 
        hellofresh_week = '{hellofresh_week_option}' AND
        country = '{entity}' AND
        ((country in ('NZ') AND box_size in (3,4,6)) OR
        (country in ('AU','AO') AND box_size in (4,6)))
        GROUP BY 1,2,3,4,5,6


        UNION ALL

        SELECT 
        hellofresh_week,
        country,
        dc,
        recipe_index,
        recipe_family,
        CASE WHEN recipe_family like '%addons%' THEN 'addons' ELSE 'meals' END AS recipe_type,
        2 as recipe_size,
        sum(kit_count) as kit_count
        FROM anz_finance_app.anz_orders_slot_details 
        WHERE 
        hellofresh_week = '{hellofresh_week_option}'
        AND box_size = 0 AND
        country = '{entity}' 
        GROUP BY 1,2,3,4,5,6
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
          FROM anz_finance_app.anz_kraken_operations_historical
          GROUP BY 1,2,3
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

def run_kraken_trend_supplier_split_error():
    query = f"""
          SELECT 
          version, bob_entity_code, hellofresh_week , count_error
          FROM anz_finance_app.anz_kraken_operations_historical_supplier_split_errors 
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df


def run_kraken_null_price_error(version, week):
    query = f"""
        SELECT 
        version, hellofresh_week, bob_entity_code, sku_code, sku_name, supplier_code, supplier_name, dc, dc_price, nation_price, period_avg_price, applied_price, line_count, forecast_sku_quantity, total_costs
        FROM anz_finance_app.anz_null_price_errors
        WHERE version = '{version}'
        AND hellofresh_week = '{week}'
        ORDER BY
        1,2,3,4,5,6,7,8
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df



def run_kraken_null_price_error_trends(version):
    query = f"""
        SELECT 
        hellofresh_week, 
        bob_entity_code,
        SUM(total_costs) as total_costs
        FROM anz_finance_app.anz_null_price_errors
        WHERE version = '{version}'
        GROUP BY 1,2
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df

def run_kraken_cpk(week, entity):
    query = f"""
        WITH KITCOUNT_DC (
            SELECT 
            slot,
            hellofresh_week,
            recipe_size,
            dc,
            version,
            bob_entity_code,
            AVG(forecast_kitcount) as kitcount
            FROM anz_finance_app.anz_kraken_operations_historical_cpk 
            WHERE hellofresh_week = '{week}'
            AND bob_entity_code = '{entity}'
            GROUP BY
            1,2,3,4,5,6
        ),
        KITCOUNT (
            SELECT 
            slot,
            hellofresh_week,
            recipe_size,
            version,
            bob_entity_code,
            SUM(kitcount) as kitcount
            FROM KITCOUNT_DC
            GROUP BY
            1,2,3,4,5
        )

        SELECT
        R.slot, 
        R.recipe_title, 
        concat(R.recipe_size,"P") as recipe_size, 
        R.version, 
        R.bob_entity_code, 
        R.hellofresh_week, 
        K.kitcount,
        R.primary_tag,
        R.product_type,
        SUM(cpk) as cpk
        FROM anz_finance_app.anz_kraken_operations_historical_cpk R
        LEFT JOIN KITCOUNT K ON R.slot = K.slot AND R.recipe_size = K.recipe_size AND R.version = K.version AND R.bob_entity_code = K.bob_entity_code
        AND R.hellofresh_week = K.hellofresh_week
        WHERE R.hellofresh_week = '{week}'
        AND R.bob_entity_code = '{entity}'
        GROUP BY 1,2,3,4,5,6,7,8,9
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df




def run_kraken_cpk_primary_tag(week, entity):
    query = f"""
        WITH KITCOUNT_DC (
            SELECT 
            slot,
            hellofresh_week,
            recipe_size,
            dc,
            version,
            bob_entity_code,
            AVG(forecast_kitcount) as kitcount
            FROM anz_finance_app.anz_kraken_operations_historical_cpk 
            WHERE hellofresh_week = '{week}'
            AND bob_entity_code = '{entity}'
            GROUP BY
            1,2,3,4,5,6
        ),
        KITCOUNT (
            SELECT 
            slot,
            hellofresh_week,
            recipe_size,
            version,
            bob_entity_code,
            SUM(kitcount) as kitcount
            FROM KITCOUNT_DC
            GROUP BY
            1,2,3,4,5
        ),
        KITCOUNT_PT (
            SELECT
            L.hellofresh_week,
            L.bob_entity_code,
            P.primary_tag,
            L.recipe_size,
            L.version,
            SUM(L.kitcount) as kitcount
            FROM KITCOUNT L
            LEFT JOIN (SELECT DISTINCT country, hellofresh_week, recipe_slot, primary_tag, product_type FROM anz_product_anon.staging_primary_tags WHERE primary_tag <> 'not mapped') P
                        ON P.country            = L.bob_entity_code
                            AND P.hellofresh_week = L.hellofresh_week
                            AND P.recipe_slot     = L.slot
            GROUP BY 1,2,3,4,5
        )

        SELECT
        R.primary_tag, 
        concat(R.recipe_size,"P") as recipe_size, 
        R.version, 
        R.bob_entity_code, 
        R.hellofresh_week, 
        K.kitcount,
        R.product_type,
        SUM(cpk_primary_tag) as cpk
        FROM anz_finance_app.anz_kraken_operations_historical_cpk R
        LEFT JOIN KITCOUNT_PT K ON R.primary_tag = K.primary_tag AND R.recipe_size = K.recipe_size AND R.version = K.version AND R.bob_entity_code = K.bob_entity_code
        AND R.hellofresh_week = K.hellofresh_week
        WHERE R.hellofresh_week = '{week}'
        AND R.bob_entity_code = '{entity}'
        GROUP BY 1,2,3,4,5,6,7
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df


def run_kraken_cpk_product_type(week, entity):
    query = f"""
        WITH KITCOUNT_DC (
            SELECT 
            slot,
            hellofresh_week,
            recipe_size,
            dc,
            version,
            bob_entity_code,
            AVG(forecast_kitcount) as kitcount
            FROM anz_finance_app.anz_kraken_operations_historical_cpk 
            WHERE hellofresh_week = '{week}'
            AND bob_entity_code = '{entity}'
            GROUP BY
            1,2,3,4,5,6
        ),
        KITCOUNT (
            SELECT 
            slot,
            hellofresh_week,
            recipe_size,
            version,
            bob_entity_code,
            SUM(kitcount) as kitcount
            FROM KITCOUNT_DC
            GROUP BY
            1,2,3,4,5
        ),
        KITCOUNT_TYPE (
            SELECT
            L.hellofresh_week,
            L.bob_entity_code,
            P.product_type,
            L.recipe_size,
            L.version,
            SUM(L.kitcount) as kitcount
            FROM KITCOUNT L
            LEFT JOIN (SELECT DISTINCT country, hellofresh_week, recipe_slot, primary_tag, product_type FROM anz_product_anon.staging_primary_tags WHERE primary_tag <> 'not mapped') P
                        ON P.country            = L.bob_entity_code
                            AND P.hellofresh_week = L.hellofresh_week
                            AND P.recipe_slot     = L.slot
            GROUP BY 1,2,3,4,5
        )

        SELECT
        R.product_type, 
        concat(R.recipe_size,"P") as recipe_size, 
        R.version, 
        R.bob_entity_code, 
        R.hellofresh_week, 
        K.kitcount,
        SUM(cpk_product_type) as cpk
        FROM anz_finance_app.anz_kraken_operations_historical_cpk R
        LEFT JOIN KITCOUNT_TYPE K ON R.product_type = K.product_type AND R.recipe_size = K.recipe_size AND R.version = K.version AND R.bob_entity_code = K.bob_entity_code
        AND R.hellofresh_week = K.hellofresh_week
        WHERE R.hellofresh_week = '{week}'
        AND R.bob_entity_code = '{entity}'
        GROUP BY 1,2,3,4,5,6
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df


def run_kraken_slot_details(week, entity, slot):
    query = f"""
        SELECT 
        version,
        sku_code,
        sku_name,
        concat(recipe_size,"P") as recipe_size, 
        SUM(forecast_sku_quantity) as forecast_sku_quantity,
        SUM(forecast_total_cost) as forecast_total_cost,
        SUM(forecast_total_cost/forecast_sku_quantity) as sku_unit_cost
        FROM anz_finance_app.anz_kraken_operations_historical
        WHERE hellofresh_week = '{week}'
        AND bob_entity_code = '{entity}'
        AND slot = '{slot}'
        GROUP BY 1,2,3,4
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df



def run_kraken_slot_details_primary_tag(week, entity, tag):
    query = f"""
        SELECT 
        R.version,
        R.sku_code,
        R.sku_name,
        concat(recipe_size,"P") as recipe_size, 
        SUM(R.forecast_sku_quantity) as forecast_sku_quantity,
        SUM(R.forecast_total_cost) as forecast_total_cost,
        SUM(R.forecast_total_cost/R.forecast_sku_quantity) as sku_unit_cost
        FROM anz_finance_app.anz_kraken_operations_historical R
        LEFT JOIN (SELECT DISTINCT country, hellofresh_week, recipe_slot, primary_tag, product_type FROM anz_product_anon.staging_primary_tags WHERE primary_tag <> 'not mapped') P
            ON P.country            = R.bob_entity_code
                AND P.hellofresh_week = R.hellofresh_week
                AND P.recipe_slot     = R.slot
        WHERE R.hellofresh_week = '{week}'
        AND R.bob_entity_code = '{entity}'
        AND P.primary_tag = '{tag}'
        GROUP BY 1,2,3,4
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    return df
