import streamlit as st
from utils.db import verify_reset_token, reset_user_password
import urllib.parse
import time

st.set_page_config(
    page_title="Reset Password",
    page_icon=":bulb:",
    layout="centered",
    initial_sidebar_state="collapsed"
)
st.markdown("""
  <style>
        [data-testid="stSidebarNav"] { display: none !important; }
    </style>
""", unsafe_allow_html=True)

if "reset_loading" not in st.session_state:
    st.session_state.reset_loading = False


query_params = st.query_params
token = query_params.get("token")

if not token:
    st.error("❌ Invalid or missing reset token.")
    st.stop()

email = verify_reset_token(token)  # Your backend should decode & validate
if not email:
    st.error("❌ Invalid or expired reset token.")
    st.stop()

st.title("🔒 Reset Your Password")
st.write(f"For: **{email}**")

password = st.text_input("New Password", type="password")
confirm = st.text_input("Confirm Password", type="password")

if not st.session_state.reset_loading:
    if st.button("🔁 Reset Password", disabled=st.session_state.reset_loading):
        if not password or not confirm:
            st.error("All fields are required.")
        elif password != confirm:
            st.error("Passwords do not match.")
        elif len(password) < 6:
            st.error("Password must be at least 6 characters.")
        else:
            st.session_state.reset_loading = True
            st.rerun()
else:
    with st.spinner("Updating password..."):
        try:
            reset_user_password(email, password, token)
            st.success("✅ Your password has been updated!")
            st.balloons()
            time.sleep(3)
            st.session_state.reset_loading = False
            st.switch_page("home.py")
        except Exception as e:
            st.session_state.reset_loading = False
            st.error(f"❌ Failed to reset password: {e}")