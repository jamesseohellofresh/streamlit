import streamlit as st
import secrets
import datetime
from utils.db import email_exists, create_reset_token
import smtplib
from email.message import EmailMessage
import uuid
import time

def send_reset_email(recipient_email: str, reset_link: str):
    # Gmail credentials (use Streamlit secrets or environment vars)
    GMAIL_USER = st.secrets["gmail"]["user"]
    GMAIL_PASS = st.secrets["gmail"]["password"]

    # Email content
    msg = EmailMessage()
    msg["Subject"] = "🔐 Reset Your HelloFresh Finance BI Portal Password"
    msg["From"] = GMAIL_USER
    msg["To"] = recipient_email
    msg.set_content(f"""
        Hi,

        You requested a password reset for your HelloFresh Finance BI Portal account.

        Click the link below to reset your password:
        {reset_link}

        This link will expire in 30 minutes.

        If you didn't request this, you can ignore this email.

        Thanks,  
        Finance BI Portal
    """)

    # Send email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_USER, GMAIL_PASS)
            smtp.send_message(msg)
        st.toast("✅ Reset link sent via email.")
    except Exception as e:
        st.error(f"❌ Failed to send reset email: {e}")

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


# --- Init session state ---
if "reset_loading" not in st.session_state:
    st.session_state.reset_loading = False

    
st.title("🔑 Forgot Your Password?")
st.write("Enter your email address and we’ll send you a reset link.")

email = st.text_input("📧 Email")

if not st.session_state.reset_loading:
    if st.button("⌲ Send Reset Link", disabled=st.session_state.reset_loading):
        if not email:
            st.error("Please enter your email.")
        elif not email_exists(email):
            st.error("No account found with this email.")
        else:
            st.session_state.reset_loading = True
            st.rerun()
else:
    with st.spinner("Sending reset link..."):
        try:
            token = str(uuid.uuid4())
            create_reset_token(email, token)

            reset_link = f"https://anz-finance-portal-2322873405429936.aws.databricksapps.com/reset_password?token={token}"
            # reset_link = f"http://localhost:8501/reset_password?token={token}"
            send_reset_email(email, reset_link)

            st.success("✅ Reset link sent! Check your email. Redirecting to login...")
            time.sleep(3)
            st.session_state.reset_loading = False
            st.switch_page("home.py")

        except Exception as e:
            st.session_state.reset_loading = False
            st.error(f"❌ Failed to send email: {e}")

