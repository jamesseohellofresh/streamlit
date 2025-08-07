import streamlit as st
from databricks import sql
import pandas as pd
import requests
import os
import time
import datetime
import bcrypt
import uuid
import datetime

DATABRICKS_HOST = st.secrets['databricks']['host']
HTTP_PATH = st.secrets['databricks']['http_path']
ACCESS_TOKEN = st.secrets['databricks']['token']

os.environ["DATABRICKS_HOST"] = f"https://{st.secrets['databricks']['host']}"
os.environ["DATABRICKS_TOKEN"] = st.secrets['databricks']['token']

# API endpoint
apiurl = f"{os.environ['DATABRICKS_HOST']}/api/2.1/jobs/runs/submit"



if "jobs" not in st.session_state:
    st.session_state.jobs = {} 

def runtestnotebook(path, cluster):
    # JSON payload (replace cluster_id and notebook_path)
    payload = {
        "run_name": "Streamlit-triggered-job",
        "tasks": [{
            "task_key": "run_notebook",
            "notebook_task": {
                "notebook_path": f"{path}"
            },
            "existing_cluster_id": f"{cluster}",  # Use cluster ID, not name
            "timeout_seconds": 3600
        }]
    }
    try:
        response = requests.post(
            apiurl,
            headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"},
            json=payload
        )
        response.raise_for_status()  # Raise HTTP errors

        run_id = response.json()["run_id"]
        st.session_state.jobs[run_id] = {"status": "SUBMITTED", "result": None, "notified": False}
        st.success(f"Job submitted! Run ID: {run_id}")

        # Start polling in the background (or you can trigger via a button)
        check_job_status(run_id)
    except requests.exceptions.RequestException as e:
        st.error(f"API request failed: {str(e)}")
    except KeyError as e:
        st.error(f"Key error: {str(e)}")


def check_job_status(run_id):
    status_url = f"{os.environ['DATABRICKS_HOST']}/api/2.1/jobs/runs/get?run_id={run_id}"
    try:
        status_response = requests.get(
            status_url,
            headers={"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}
        )
        status_response.raise_for_status()
        status = status_response.json()
        life_cycle_state = status["state"]["life_cycle_state"]
        result_state = status["state"].get("result_state", None)
        st.session_state.jobs[run_id]["status"] = life_cycle_state
        st.session_state.jobs[run_id]["result"] = result_state
        # Set notified flag to False so notification is shown
        st.session_state.jobs[run_id]["notified"] = False
    except Exception as e:
        st.error(f"Failed to check status for job {run_id}: {e}")


def get_connection():
    return sql.connect(
        server_hostname= DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
        _verify_ssl="/Users/james.seo/Downloads/databricks_root.cer"
    )

def validate_login(email, password):
    conn= get_connection()
    cursor = conn.cursor()
    query = """
        SELECT password_hash FROM hive_metastore.anz_finance_app.users 
        WHERE email = ? AND line_del = false
    """
    cursor.execute(query, (email,))
    result = cursor.fetchone()
    conn.close()

    if result:
        stored_hash = result[0]
        return bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
    return False


# 데이터 쿼리 함수
def load_tables():
    query = f"""

        WITH BOX_COST
        (

        WITH SKU_UNIT_COST
        (
            SELECT 
            C.country,
            C.hellofresh_week,
            C.recipe_index as slot,
            CASE WHEN C.dc= 'TFB WA' THEN 'Perth'
                WHEN C.dc = 'NZ-unknown' THEN 'NZ'
                ELSE C.dc END as dc,
            C.persons as box_size,
            M.total_kits,
            sum(C.cost_value) as total_costs,
            CASE WHEN M.total_kits =0 THEN 0 ELSE sum(C.cost_value)/M.total_kits END as sku_cost
            from uploads.gor_uploaded_costs C --limit 100
            LEFT JOIN (

                SELECT 
                O.country,
                O.hellofresh_week,
                O.recipe_index,
                P.box_size,
                CASE WHEN o.country = 'NZ' THEN 'NZ' 
                    WHEN OW.courier = 'WA BC' THEN 'Perth'
                    ELSE 'Sydney'
                    END as dc     ,
                SUM(O.quantity) as total_kits
                FROM fact_tables.recipes_ordered O
                INNER JOIN hive_metastore.public_edw_business_mart_live.order_line_items L ON O.box_id = L.order_line_items_id
                INNER JOIN hive_metastore.global_bi_business.product_dimension P ON L.product_sku = P.product_sku AND L.bob_entity_code = P.bob_entity_code AND P.bob_entity_code in ('AU', 'AO','NZ')
                INNER  JOIN restricted_katana_live.boxes_output_enriched_ow  OW ON O.box_id = OW.box_id AND OW.country in ('au', 'ao','nz')
                WHERE 
                O.hellofresh_week = '2025-W20'
                AND O.country in ('AU','NZ','AO')
                GROUP BY
                1,2,3,4,5

            ) M ON C.country = M.country AND C.hellofresh_week = M.hellofresh_week AND C.recipe_index = M.recipe_index AND C.dc = M.dc AND C.persons = M.box_size
            WHERE C.country in ('AO', 'AU', 'NZ')
            AND C.hellofresh_week = '2025-W20'
            AND C.cost_type = 'Direct'
            AND C.cost_center = 'Ingredients'
            AND C.cost_value > 0
            GROUP BY 
            1,2,3,4,5,6
        )

        SELECT
        O.country,
        O.box_id,
        O.recipe_index,
        O.quantity,
        O.composite_order_id,
        O.hellofresh_week,
        C.sku_cost
        FROM fact_tables.recipes_ordered O
        INNER JOIN hive_metastore.public_edw_business_mart_live.order_line_items L ON O.box_id = L.order_line_items_id
        INNER  JOIN restricted_katana_live.boxes_output_enriched_ow  OW ON O.box_id = OW.box_id AND OW.country in ('au', 'ao','nz')
        INNER JOIN hive_metastore.global_bi_business.product_dimension P ON L.product_sku = P.product_sku AND L.bob_entity_code = P.bob_entity_code AND P.bob_entity_code in ('AU', 'AO','NZ')
        LEFT JOIN SKU_UNIT_COST C ON O.country = C.country AND O.hellofresh_week = C.hellofresh_week AND O.recipe_index = C.slot AND P.box_size = C.box_size 
        AND CASE WHEN o.country = 'NZ' THEN 'NZ' 
                    WHEN OW.courier = 'WA BC' THEN 'Perth'
                    ELSE 'Sydney'
                    END   = C.dc
        WHERE 
        O.hellofresh_week = '2025-W20'
        AND O.country in ('AU','NZ','AO')
        )

        SELECT
        L.bob_entity_code,
        L.composite_order_id,
        L.order_line_items_id,
        P.box_size,
        P.number_of_recipes,
        SUM(order_item_revenue_excl_sales_tax) as order_item_revenue_excl_sales_tax,
        SUM(shipping_revenue_excl_sales_tax) as shipping_revenue_excl_sales_tax,
        SUM(order_item_net_revenue) as order_item_net_revenue,
        BC.total_direct_costs as total_direct_costs,
        ROUND(1-(BC.total_direct_costs/SUM(order_item_net_revenue)),2) as net_p1c_margin 
        FROM hive_metastore.public_edw_business_mart_live.order_line_items  L 
        INNER JOIN hive_metastore.global_bi_business.product_dimension P ON L.product_sku = P.product_sku AND L.bob_entity_code = P.bob_entity_code AND P.bob_entity_code in ('AU', 'AO','NZ')
        LEFT JOIN (
            SELECT
            box_id,
            sum(C.sku_cost * C.quantity) as total_direct_costs
            FROM BOX_COST C
            group by 1
            ) BC ON L.order_line_items_id = BC.box_id 
        
        WHERE
        L.hellofresh_delivery_week = '2025-W20'
        AND L.bob_entity_code in ('AU','NZ','AO')
        GROUP BY 
        L.bob_entity_code,
        L.composite_order_id,
        L.order_line_items_id,
        P.box_size,
        P.number_of_recipes,
        BC.total_direct_costs 


  
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    # Display the dataframe
    st.dataframe(df)

def fetch_inventory_items():
    conn = get_connection()
    query = """
    SELECT item_id, item_code
    FROM ibizlink.inventory_items
    WHERE status_id = 10 AND line_del = 0
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def email_exists(email: str) -> bool:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1
        FROM hive_metastore.anz_finance_app.users
        WHERE email = ? AND line_del = false
        LIMIT 1
    """, (email,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def register_user(email: str, password: str, department: str, entity_code: str):
    conn = get_connection()
    cursor = conn.cursor()
    hashed_pw = hash_password(password)
    cursor.execute(
        """
        INSERT INTO hive_metastore.anz_finance_app.users 
        (email, password_hash, department, default_bob_entity_code, line_del)
        VALUES (?, ?, ?, ?, false)
        """,
        (email, hashed_pw, department, entity_code)
    )
    conn.commit()
    conn.close()


def validate_login_from_db(email: str, password: str) -> bool:
    conn = get_connection()
    cursor = conn.cursor()
    query = """
        SELECT password_hash FROM hive_metastore.anz_finance_app.users 
        WHERE email = ? AND line_del = false
    """
    cursor.execute(query, (email,))
    result = cursor.fetchone()
    conn.close()

    if result:
        stored_hash = result[0]
        return bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
    return False


def create_reset_token(email: str, token: str) -> str:
    
    expires_at = datetime.datetime.now(datetime.timezone.utc)+ datetime.timedelta(minutes=30)

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO hive_metastore.anz_finance_app.password_reset_tokens (email, token, expires_at, is_used)
        VALUES (?, ?, ?, false)
    """, (email, token, expires_at))
    conn.commit()
    conn.close()

    return token

def verify_reset_token(token: str) -> str | None:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT email
        FROM hive_metastore.anz_finance_app.password_reset_tokens
        WHERE token = ? AND expires_at > current_timestamp() AND is_used = false
        LIMIT 1
    """, (token,))
    result = cursor.fetchone()
    conn.close()

    if result:
        return result[0]  # email
    return None

def mark_token_as_used(token: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE hive_metastore.anz_finance_app.password_reset_tokens
        SET is_used = true
        WHERE token = ?
    """, (token,))
    conn.commit()
    conn.close()

def reset_user_password(email: str, new_password: str, token: str):
    hashed_pw = hash_password(new_password)

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE hive_metastore.anz_finance_app.users
        SET password_hash = ?
        WHERE email = ? AND line_del = false
    """, (hashed_pw, email))
    conn.commit()
    conn.close()

    # Mark token as used
    mark_token_as_used(token)