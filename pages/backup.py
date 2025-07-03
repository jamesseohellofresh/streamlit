import streamlit as st
from utils.db import (
     load_tables,
     fetch_inventory_items,
     runtestnotebook,
     check_job_status
)
from datetime import datetime,timedelta
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="📊 Dashboard", layout="wide")


st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)

if not st.user.is_logged_in:
    st.switch_page("home.py")



# --- UI ---




st.sidebar.markdown(f"User :  {st.user.email}")

# 사이드바에 날짜 선택 위젯 추가
selected_date = st.sidebar.date_input(
    "Select Date", 
    min_value=datetime(2020, 1, 1), 
    max_value=datetime.today(), 
    value=datetime.today()
)

if st.sidebar.button("Run Noteboock"):
    runtestnotebook('/Squad-AU-Finops/pipeline/streamlit_test',st.secrets["databricks"]["anz_data_cluster_id"])

# today = datetime.today()
# one_year_ago = today - timedelta(days=700)
# # Make sure the date range is persisted across reruns using session_state
# if "start_date" not in st.session_state:
#     st.session_state.start_date = today - timedelta(days=30)  # Default value: last 30 days

# if "end_date" not in st.session_state:
#     st.session_state.end_date = today  # Default value: today

# 날짜 범위를 슬라이더로 선택
# start_date, end_date = st.sidebar.slider(
#     "Select Date Range",
#     # min_value=one_year_ago,
#     max_value=today,
#     value=(st.session_state.start_date, st.session_state.end_date),  # 기본값: 마지막 30일
#     format="YYYY-MM-DD"
# )


# --- 아이템 조회 ---
#df_inventory_items = fetch_inventory_items()
#item_codes = df_inventory_items['item_code'].tolist()

# selected_items = st.sidebar.multiselect(
#     "Select Item(s)", 
#     options=item_codes,  # 선택 가능한 옵션
# )
# selected_item_ids=""

# if selected_items:
#     selected_item_ids = df_inventory_items[df_inventory_items['item_code'].isin(selected_items)]['item_id'].tolist()

# Fetch the data from SQL
# load_tables()
#load_tables(selected_date.strftime('%Y-%m-%d'), selected_date.strftime('%Y-%m-%d'), selected_item_ids)



# Check status of all jobs (polling can be improved for production)
for run_id in list(st.session_state.jobs.keys()):
    # Only check if job is not finished
    if st.session_state.jobs[run_id]["status"] not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        check_job_status(run_id)

# Show notifications for completed jobs
for run_id, job in st.session_state.jobs.items():
    if job["status"] == "TERMINATED" and not job["notified"]:
        if job["result"] == "SUCCESS":
            st.success(f"🎉 Job {run_id} is done! The task completed successfully.")
        elif job["result"] == "FAILED":
            st.error(f"❌ Job {run_id} failed. Please check the Databricks job logs.")
        else:
            st.info(f"Job {run_id} finished with status: {job['result']}")
        # Mark as notified so message persists only once
        job["notified"] = True



jobs_running = any(
    job["status"] not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
    for job in st.session_state.jobs.values()
)
if jobs_running:
    st_autorefresh(interval=5000, key="jobrefresh")