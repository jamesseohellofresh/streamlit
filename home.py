import streamlit as st
import base64
from utils.db import validate_login
from streamlit_js_eval import streamlit_js_eval

# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance Portal",
    page_icon=":bulb:",
    layout="centered"
)

# --- Custom Styles ---
def inject_styles():
    st.markdown("""
        <style>
            [data-testid="stSidebarNav"], [data-testid="stSidebar"] { display: none !important; }
        </style>
    """, unsafe_allow_html=True)

inject_styles()

# --- Utility Functions ---


def show_logo(margin_bottom="4rem"):
    col1, col2, col3 = st.columns([1, 4, 1])
    with col2:
        st.markdown(
            f"""
            <div style='text-align: center;'>
                <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://assets-us-01.kc-usercontent.com/7af951a6-2a13-004b-f0eb-a87382a5b2e7/d8ba1d2b-bd49-4f1f-a11f-9cb7935bd450/Hello%20Fresh.png?fm=png&auto=format&w=1024' width="200" style="margin-bottom: 4px;"/>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )


# --- 1. 로그인 전: 안내/소개 페이지 ---
if not st.user.is_logged_in:
    show_logo()
    # 타이틀 및 설명
    st.markdown(
        """
        <div style='display: flex; align-items: center; gap: 10px;'>
            <span style='font-size: 2.2em;'>💡</span>
            <span style='font-size: 2.2em; font-weight: bold;'>Hellofresh Finance Portal</span>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.write(
    """
        **Welcome to the Hellofresh Finance Portal!**

        This portal is your secure gateway to powerful data analysis and visualization tools, available exclusively to authenticated users.

        After logging in, you can:
        - Explore and analyze a wide range of business data
        - Access tailored reports and interactive dashboards
        - Gain actionable insights to support your decision-making

        All features are protected by robust authentication, ensuring your data remains safe and confidential.

        """
    )

    col1, col2 = st.columns([1,1])
    with col1:
        st.markdown('<div style="margin-bottom: 32px;"></div>', unsafe_allow_html=True)
        if st.button("🔐 Login With Google", key="login_btn", use_container_width=True):
            user = st.login()
        st.stop()


st.session_state.user_email = st.user.email
st.session_state.user_name = st.user.name

show_logo(margin_bottom="2rem")
# 도메인 선택 (최초 1회)
if "user_account" not in st.session_state:
    check_login = validate_login(st.user.email)
    if check_login is None:
        st.error("No service available for your account.")
        st.stop()
    else:
        st.session_state.user_account = check_login[0]
        #st.session_state.domain_code = chosen["domain_code"]
        #st.session_state.target_database = chosen["target_database"]
        #save_domain_to_localstorage(chosen["domain_code"], chosen["target_database"])
        st.rerun()

# 메인 UI
col1, col2 = st.columns([7, 2])
with col1:
    st.markdown(f"""
        <div style='text-align: left;'>
            <div style='font-size: 16px; color: grey; margin-bottom: 20px;'>
                Account: <code>{st.session_state.user_email}</code> &nbsp;|&nbsp;
                Domain: <b>{st.session_state.user_name}</b> &nbsp;
            </div>
        </div>
    """, unsafe_allow_html=True)
with col2:
    if st.button("🔓 Log out"):
        st.logout()

st.markdown('<div style="margin-bottom: 42px;"></div>', unsafe_allow_html=True)
# Navigation Buttons
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("Dashboard", key="dashboard_btn", use_container_width=True):
        st.switch_page("pages/dashboard.py")
with col2:
    if st.button("Reports", key="reports_btn", use_container_width=True):
        st.switch_page("pages/reports.py")
with col3:
    if st.button("Settings", key="settings_btn", use_container_width=True):
        st.switch_page("pages/settings.py")


