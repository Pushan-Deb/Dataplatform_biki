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

from ui.config import (
    KEYCLOAK_SERVER, REALM, CLIENT_ID, CLIENT_SECRET,
    REDIRECT_URI, MICROSOFT_IDP_ALIAS
)

AUTH_URL = f"{KEYCLOAK_SERVER}/realms/{REALM}/protocol/openid-connect/auth"
TOKEN_URL = f"{KEYCLOAK_SERVER}/realms/{REALM}/protocol/openid-connect/token"
LOGOUT_URL = f"{KEYCLOAK_SERVER}/realms/{REALM}/protocol/openid-connect/logout"


def build_auth_url(idp_hint: str = None) -> str:
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": "openid profile email",
        "redirect_uri": REDIRECT_URI,
    }
    if idp_hint:
        params["kc_idp_hint"] = idp_hint
    return AUTH_URL + "?" + urllib.parse.urlencode(params)


LOGIN_PAGE_CSS = """
<style>
.login-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 3rem 1rem;
    gap: 1rem;
}
.login-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.65rem 1.6rem;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    text-decoration: none !important;
    cursor: pointer;
    width: 280px;
    justify-content: center;
}
.btn-keycloak {
    background: #4a90d9;
    color: #fff !important;
    border: none;
}
.btn-keycloak:hover { background: #3a7abf; }
.btn-microsoft {
    background: #fff;
    color: #333 !important;
    border: 1px solid #ccc;
}
.btn-microsoft:hover { background: #f5f5f5; }
.divider {
    color: #888;
    font-size: 12px;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    width: 280px;
}
.divider::before, .divider::after {
    content: "";
    flex: 1;
    border-bottom: 1px solid #ccc;
}
</style>
"""

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
    st.markdown(LOGIN_PAGE_CSS, unsafe_allow_html=True)
    st.title("Data Platform Console")
    st.info("Please login to continue")

    keycloak_url = build_auth_url()
    microsoft_url = build_auth_url(idp_hint=MICROSOFT_IDP_ALIAS)

    st.markdown(f"""
    <div class="login-container">
        <a href="{keycloak_url}" class="login-btn btn-keycloak">🔐&nbsp; Login with Keycloak</a>
        <div class="divider">or</div>
        <a href="{microsoft_url}" class="login-btn btn-microsoft">
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 21 21">
              <rect x="1" y="1" width="9" height="9" fill="#f25022"/>
              <rect x="11" y="1" width="9" height="9" fill="#7fba00"/>
              <rect x="1" y="11" width="9" height="9" fill="#00a4ef"/>
              <rect x="11" y="11" width="9" height="9" fill="#ffb900"/>
            </svg>
            &nbsp;Login with Microsoft
        </a>
    </div>
    """, unsafe_allow_html=True)
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
