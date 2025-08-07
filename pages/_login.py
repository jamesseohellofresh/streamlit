import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager
from utils.db import validate_login # Assuming this function exists and works
import os
import time
# --- Page Config ---
st.set_page_config(
    page_title="Login",
    page_icon=":bulb:",
    layout="centered",
    initial_sidebar_state="collapsed"
)
st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)

def show_logo(margin_bottom="1rem"):
    st.markdown(
            f"""
            <div style='text-align: center;'>
                <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://media.hellofresh.com/w_1920,q_auto,f_auto,c_limit,fl_lossy/hellofresh_website/de/cms/HF200728_BIG_IDEA_OOH_BOX_MB__47_new_Logo_high.jpg' width="250" style="margin-bottom: 4px;"/>
                </div>
                 <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://media.hellofresh.com/f_auto,fl_lossy,q_auto/hellofresh_website/au/cms/SEO/RYAN5551_high.jpg' width="250" style="margin-bottom: 4px;"/>
                </div>
                 <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://media.hellofresh.com/f_auto,fl_lossy,q_auto/hellofresh_website/fr/cms/SEO/meat-dishes2.jpg' width="250" style="margin-bottom: 4px;"/>
                </div>
                 <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://media.hellofresh.com/f_auto,fl_lossy,q_auto/hellofresh_website/fr/cms/landing_pages/HF_1_Man_Delivery_750x600.jpg' width="250" style="margin-bottom: 4px;"/>
                </div>
                 <div style="display: flex; align-items: center; justify-content: center;margin-bottom: {margin_bottom};">
                    <img src='https://media.hellofresh.com/w_1920,q_auto,f_auto,c_limit,fl_lossy/hellofresh_website/es/cms/SEO/hellofresh%20box/b171546a-dc69-400d-b382-d94af321e98e.jpg' width="250" style="margin-bottom: 4px;"/>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )


if "login_loading" not in st.session_state:
    st.session_state.login_loading = False
if "login_failed" not in st.session_state:
    st.session_state.login_failed = False
if "login_error_msg" not in st.session_state:
    st.session_state.login_error_msg = None
    
# Disable fields if processing or done
form_disabled = st.session_state.login_loading


# --- COOKIE MANAGER SETUP ---
# This should be on top of your script.
# The password should be set as a secret environment variable.
cookies = EncryptedCookieManager(
    prefix="hellofresh/finance/bi-portal",
    password=os.environ.get("COOKIES_PASSWORD", "a_default_password_for_testing"),
)

if not cookies.ready():
    # Wait for the component to load and send us current cookies.
    st.stop()

# --- REDIRECT IF ALREADY LOGGED IN ---
# Check if the user's email is already in the cookies.
if not cookies.get("user_email") in [None, "{}", "null", ""]:
    st.switch_page("home.py")


# --- LAYOUT DEFINITION ---
col1, col2,col3, col4 = st.columns([20,1,1,9], gap="small")

with col1:

    st.markdown(
        """
        <div style='display: flex; align-items: center; gap: 10px; margin-bottom:20px;'>
            <span style='font-size: 1.7em;'>💡</span>
            <span style='font-size: 1.7em; font-weight: bold;'>Hellofresh Finance BI Portal</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    with st.form("login_form"):
        email = st.text_input("📧 Email", disabled=form_disabled,)
        password = st.text_input("🔑 Password", type="password",disabled=form_disabled)
        
        col_login, col_reset = st.columns([1, 1])
        with col_login:
            submit = st.form_submit_button("🔐 Login", disabled=form_disabled, use_container_width=True)
        with col_reset:
            reset = st.form_submit_button("🔑 Forgot Password?", disabled=form_disabled, use_container_width=True)
        
        signup = st.form_submit_button("📝 Sign Up", disabled=form_disabled, use_container_width=True)
        if submit:
            st.session_state.login_loading = True
            st.rerun()

        if "login_error_msg" in st.session_state:
            if st.session_state.login_error_msg and not st.session_state.login_loading:
                st.error(st.session_state.login_error_msg)
            del st.session_state.login_error_msg

        if st.session_state.login_loading:
            with st.spinner("Verifying credentials..."):
                if validate_login(email, password):
                    # If login is successful, set the cookies.
                    cookies['user_email'] = email
                    # We'll use the email as the user's name for display purposes.
                    cookies['user_name'] = email 
                    cookies.save() # Save the cookies immediately
                    st.success("✅ Login successful! Redirecting...")
                    time.sleep(1)
                    st.switch_page("home.py")
                else:
                    st.session_state.login_failed = True
                    st.session_state.login_loading = False
                    st.session_state.login_error_msg  ="❌ Invalid credentials"
                    st.rerun()

        # Show error only if login failed
        if st.session_state.login_failed:
            st.session_state.login_error_msg  ="❌ Invalid credentials"


        if signup:
            st.session_state.signup_mode = True
            st.switch_page("pages/_signup.py")

        if reset:
            st.session_state.reset_mode = True
            st.switch_page("pages/_forgot_password.py")

        st.markdown(
        """
        <div style='display: flex; align-items: center; gap: 10px; margin-bottom:40px;'>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.write("""
        This portal is your secure gateway to powerful data analysis and visualization tools, available exclusively to authenticated users.

        After logging in, you can:
        - Explore and analyze a wide range of business data
        - Access tailored reports and interactive dashboards
        - Gain actionable insights to support your decision-making

        All features are protected by robust authentication, ensuring your data remains safe and confidential.
    """)

with col4:
    show_logo("0.5rem")

