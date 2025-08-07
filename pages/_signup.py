import bcrypt
import streamlit as st
from utils.db import (
    register_user,
    email_exists
)
import time

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


# Initialize flags
if "signup_loading" not in st.session_state:
    st.session_state.signup_loading = False
if "signup_success" not in st.session_state:
    st.session_state.signup_success = False

# Disable fields if processing or done
form_disabled = st.session_state.signup_loading or st.session_state.signup_success

# --- Page Config ---
st.set_page_config(
    page_title="HelloFresh Finance BI Portal",
    page_icon=":bulb:",
    layout="centered",
    initial_sidebar_state="collapsed"
)
st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)



# --- LAYOUT DEFINITION ---
col1, col2,col3, col4 = st.columns([20,1,1,9], gap="small")



with col1:
    # 🔙 Show back to login button regardless of success
    if st.button("◀️ Back to Login", disabled=st.session_state.signup_loading):
        st.session_state.signup_mode = False  # Optional
        st.switch_page("pages/_login.py")  # Adjust if your login.py is at root
        

    st.markdown(
        """
        <div style='display: flex; align-items: center; gap: 10px; margin-bottom:20px;'>
            <span style='font-size: 1.7em;'>💡</span>
            <span style='font-size: 1.7em; font-weight: bold;'>Sign Up Your Account</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    with st.form("signup_form"):
        email = st.text_input("Email",disabled=form_disabled)
        password = st.text_input("Password", type="password",disabled=form_disabled)
        confirm_password = st.text_input("Confirm Password", type="password",disabled=form_disabled)

        # Put Department and Entity Code side by side
        col1, col2 = st.columns(2)
        with col1:
            department = st.selectbox(
                "Department",
                ["Finance", "Customer & Product Strategy", "Data & Growth Strategy", "Fulfilment Operations", "Procurement / SCM", "Upstream Supply Chain"],
                disabled=form_disabled
            )
        with col2:
            entity_code = st.selectbox(
                "Entity Code",
                ["AU", "AO", "NZ"],
                disabled=form_disabled
            )

        submit = st.form_submit_button("📝  Sign Up",disabled=form_disabled)


    if submit:
        st.session_state.signup_loading = True
        st.rerun()  # refresh UI immediately with fields disabled


    # Show messages from previous rerun
    if "signup_error_msg" in st.session_state:
        st.error(st.session_state.signup_error_msg)
        del st.session_state.signup_error_msg

    if st.session_state.signup_loading and not st.session_state.signup_success:
        with st.spinner("Processing registration..."):
            if not all([email, password, confirm_password, department, entity_code]):
                st.session_state.signup_error_msg  ="🚨 All fields are required."
                st.session_state.signup_loading = False
                st.rerun()
            elif len(password) < 6:
                st.session_state.signup_error_msg = "🔐 Password must be at least 6 characters."
                st.session_state.signup_loading = False
                st.rerun()
            elif password != confirm_password:
                st.session_state.signup_error_msg  ="❌ Passwords do not match."
                st.session_state.signup_loading = False
                st.rerun()
            elif email_exists(email):
                st.session_state.signup_error_msg  = "⚠️ This email is already registered."
                st.session_state.signup_loading = False
                st.rerun()
            else:
                register_user(email, password, department, entity_code)
                st.success("✅ Account created successfully! Redirecting to home...")
                st.balloons()
                st.session_state.signup_success = True
                time.sleep(5)
                st.session_state.signup_loading = False
                st.session_state.signup_mode = False
                st.switch_page("home.py")

        # Reset loading flag if something went wrong (e.g. validation failed)
        st.session_state.signup_loading = False

with col4:
    show_logo("0.5rem")

