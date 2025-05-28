import streamlit as st
from databricks import sql

DATABRICKS_HOST = st.secrets["databricks"]["host"]
HTTP_PATH = st.secrets["databricks"]["http_path"]
ACCESS_TOKEN = st.secrets["databricks"]["token"]

def validate_login(email):
    with sql.connect(
            server_hostname= DATABRICKS_HOST,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN,
            _verify_ssl="/Users/james.seo/Downloads/databricks_root.cer"
        ) as conn:
            cursor = conn.cursor()
            query = """
                SELECT email FROM hive_metastore.anz_finance_app.users 
                WHERE email = ? AND line_del = false
            """
            cursor.execute(query, (email,))
            result = cursor.fetchone()
            print(result)
            #print(f"Connection OK. Result: {result[0]}")
            return result[0] if result else None

