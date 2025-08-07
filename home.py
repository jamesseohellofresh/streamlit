import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager
import os
import time
import json
import streamlit as st
from streamlit_tile import streamlit_tile
from utils.db import validate_login
from streamlit_js_eval import streamlit_js_eval
import time


def show_logo(margin_bottom="1rem"):
    col1, col2, col3 = st.columns([1, 4, 1])
    with col2:
        st.markdown(
            f"""
            <div style='text-align: center;'>
                <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://assets-us-01.kc-usercontent.com/7af951a6-2a13-004b-f0eb-a87382a5b2e7/d8ba1d2b-bd49-4f1f-a11f-9cb7935bd450/Hello%20Fresh.png?fm=png&auto=format&w=1024' width="150" style="margin-bottom: 4px;"/>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )


# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance BI Portal",
    page_icon=":bulb:",
    layout="wide"
)

# --- Custom Styles ---
def inject_styles():
    st.markdown("""
        <style>
            [data-testid="stSidebarNav"], [data-testid="stSidebar"] { display: none !important; }
        </style>
    """, unsafe_allow_html=True)

inject_styles()


# --- COOKIE MANAGER SETUP ---
# Must be initialized with the same parameters on every page.
cookies = EncryptedCookieManager(
    prefix="hellofresh/finance/bi-portal",
    password=os.environ.get("COOKIES_PASSWORD", "a_default_password_for_testing"),
)

if not cookies.ready():
    st.stop()

# --- AUTHENTICATION CHECK ---
# If the user_email cookie is not set, redirect to the login page.
if cookies.get("user_email") in [None, "{}", "null", ""]:
    st.warning("🔒 You are not logged in. Redirecting to login page...")
    time.sleep(1)
    st.switch_page("pages/_login.py")
    st.stop()


# Display user info and logout button in the sidebar
# with st.sidebar:
#     st.markdown(f"👤 Logged in as: **{cookies.get('user_name', 'Unknown User')}**")
#     if st.button('🔓 Log out'):
#         # ❗ Clear cookies explicitly
#         cookies["user_email"] = json.dumps({})
#         cookies["user_name"] = json.dumps({})
#         cookies.save()
#         # Small delay to ensure cookie propagation
#         time.sleep(0.5)
#         # Force rerun (redirect to login page via auth check)
#         st.rerun()


show_logo(margin_bottom="1rem")

col1, col2 = st.columns([10, 1])
with col1:
    st.markdown(f"""
        <div style='text-align: left;'>
            <div style='font-size: 16px; color: grey; margin-bottom: 20px;'>
                User : <code>{cookies.get('user_name', 'Unknown User')}</code> &nbsp;
            </div>
        </div>
    """, unsafe_allow_html=True)
with col2:
    if st.button("🔓 Log out"):
        # ❗ Clear cookies explicitly
        cookies["user_email"] = json.dumps({})
        cookies["user_name"] = json.dumps({})
        cookies.save()
        # Small delay to ensure cookie propagation
        time.sleep(0.5)
        # Force rerun (redirect to login page via auth check)
        st.rerun()



st.markdown('<div style="margin-bottom: 42px;"></div>', unsafe_allow_html=True)
# Navigation Buttons
col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    kraken_ops_btn_clicked = streamlit_tile(
        title="Kraken Ops Insight",
        description="",
        color_theme="blue",
        icon="",
        #color_theme="black", 
        height=180,

    )
    if kraken_ops_btn_clicked:
        st.switch_page("pages/krakenops.py")

with col2:       
    order_recipe_margin_btn_clicked = streamlit_tile(
        title="Order Recipe Margin",
        description="",
        color_theme="red",
        icon="",
        #color_theme="black", 
        height=180,

    )
    if order_recipe_margin_btn_clicked:
        st.switch_page("pages/orderrecipemargin.py")

with col3:
    box_count_btn_clicked = streamlit_tile(
        title="Box / Kit Count",
        description="",
        color_theme="yellow",
        icon="",
        #color_theme="black", 
        height=180,

    )
    if box_count_btn_clicked:
        st.switch_page("pages/boxcount.py")

with col4:
    finance_tools_btn_clicked = streamlit_tile(
        title="Finance Hub",
        description="",
        color_theme="green",
        icon="",
        #color_theme="black", 
        height=180,

    )

# with col2:
#     if st.button("Budget Recipe Composition", key="recipe_composition_btn", use_container_width=True):
#         st.switch_page("pages/budgetrecipecomposition.py")

# col4, col5, col6 = st.columns(3)
# with col5:
#     if st.button("Kraken Ops", key="kraken_btn", use_container_width=True):
#         st.switch_page("pages/krakenops.py")
# with col6:
#     if st.button("COGS WBR", key="cogswbr_btn", use_container_width=True):
#         st.switch_page("pages/cogswbr.py")

# Create a simple tile


# if menu_planning_btn_clicked:
#     st.switch_page("pages/menuplanning.py")
