import streamlit as st
from datetime import datetime
import pandas as pd
import uuid
from urllib.parse import quote_plus, unquote_plus, parse_qs
import urllib.parse
import re
import requests
import os
from jose import jwt
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

# ==============================
# KEYCLOAK CONFIG (loaded from .env)
# ==============================

KEYCLOAK_SERVER = os.getenv("KEYCLOAK_SERVER", "http://localhost:8090")
REALM = os.getenv("KEYCLOAK_REALM", "master")
CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "data-platform-ui")
CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("KEYCLOAK_REDIRECT_URI", "http://localhost:8501")

AUTH_URL = f"{KEYCLOAK_SERVER}/realms/{REALM}/protocol/openid-connect/auth"
TOKEN_URL = f"{KEYCLOAK_SERVER}/realms/{REALM}/protocol/openid-connect/token"
LOGOUT_URL = f"{KEYCLOAK_SERVER}/realms/{REALM}/protocol/openid-connect/logout"

def login():
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": "openid profile email",
        "redirect_uri": REDIRECT_URI,
    }
    url = AUTH_URL + "?" + urllib.parse.urlencode(params)
    st.markdown(f"[🔐 Login with Keycloak]({url})")

st.set_page_config(layout="wide", page_title="Data Platform Console")

SMALL_FONT_CSS = """
<style>
html, body, [class*="css"]  { font-size: 12px; }
textarea { font-size: 12px !important; }
input { font-size: 12px !important; }
</style>
"""
st.markdown(SMALL_FONT_CSS, unsafe_allow_html=True)

def exchange_code_for_token(code):
    data = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
    }
    response = requests.post(TOKEN_URL, data=data)
    return response.json()

SUCCESS_BOX_CSS = """
<style>
.successbox {
  padding: 0.75rem 1rem;
  border-radius: 0.5rem;
  background: rgba(0, 255, 0, 0.12);
  border: 1px solid rgba(0, 255, 0, 0.35);
}
.successbox a { text-decoration: underline; }
</style>
"""
st.markdown(SUCCESS_BOX_CSS, unsafe_allow_html=True)

def success_box(markdown_text: str):
    st.markdown(f"<div class='successbox'>{markdown_text}</div>", unsafe_allow_html=True)

# ==============================
# HANDLE AUTH FLOW
# ==============================

query_params = st.query_params

if "code" in query_params:
    if "token" not in st.session_state:
        token_response = exchange_code_for_token(query_params["code"])
        if "access_token" in token_response:
            st.session_state["token"] = token_response
            st.query_params.clear()
            st.rerun()
        else:
            st.error("❌ Token exchange failed")
            st.write(token_response)
            st.stop()

if "token" not in st.session_state:
    st.title("Data Platform Console")
    st.info("Please login to continue")
    login()
    st.stop()

# ==============================
# DECODE TOKEN
# ==============================

def decode_token(token):
    return jwt.get_unverified_claims(token)

decoded = decode_token(st.session_state["token"]["access_token"])

username = decoded.get("preferred_username")
roles = decoded.get("resource_access", {}).get(CLIENT_ID, {}).get("roles", [])

# ==============================
# MAP KEYCLOAK ROLES TO UI ROLE
# ==============================

ROLE_TEAMS = {
    "Admin": "Platform",
    "Data Engineer": "DataEng",
    "Data Analyst": "Analytics",
    "ML Engineer": "ML",
}

PRIMARY_ROLE = None

if "platform_admin" in roles:
    PRIMARY_ROLE = "Admin"
elif "data_engineer" in roles:
    PRIMARY_ROLE = "Data Engineer"
elif "data_analyst" in roles:
    PRIMARY_ROLE = "Data Analyst"
elif "ml_engineer" in roles:
    PRIMARY_ROLE = "ML Engineer"

st.session_state.logged_in = True
st.session_state.user = username
st.session_state.role = PRIMARY_ROLE
st.session_state.team = ROLE_TEAMS.get(PRIMARY_ROLE, "Unknown")

st.sidebar.write("👤 Logged in as:", username)
st.sidebar.write("🔐 Roles:", roles)

if st.sidebar.button("Logout"):
    st.session_state.clear()
    logout_params = {
        "client_id": CLIENT_ID,
        "post_logout_redirect_uri": REDIRECT_URI
    }
    logout_url = LOGOUT_URL + "?" + urllib.parse.urlencode(logout_params)
    st.markdown(f"<meta http-equiv='refresh' content='0; url={logout_url}'>",
                unsafe_allow_html=True)
    st.stop()

from ui.state import init_state
from ui.auth import qp_autologin
from ui.runtime import handle_deferred_rerun
from ui.router import router

init_state()
qp_autologin()
router()
