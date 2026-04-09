import streamlit as st
import urllib.parse
from urllib.parse import unquote_plus
from ui.config import ROLE_TEAMS
from ui.runtime import request_rerun
from ui.state import init_state


def qp_autologin():
    qp = st.query_params
    qp_user = qp.get("user", None)
    qp_role = qp.get("role", None)
    qp_team = qp.get("team", None)

    if qp_user:
        qp_user = unquote_plus(qp_user)
    if qp_role:
        qp_role = unquote_plus(qp_role)
    if qp_team:
        qp_team = unquote_plus(qp_team)

    if not st.session_state.logged_in and qp_user and qp_role:
        st.session_state.logged_in = True
        st.session_state.user = urllib.parse.unquote_plus(qp_user)
        st.session_state.role = urllib.parse.unquote_plus(qp_role)
        st.session_state.team = urllib.parse.unquote_plus(qp_team) if qp_team else ROLE_TEAMS.get(
            st.session_state.role, "Unknown")

    qp_page = qp.get("page", None)
    if qp_page:
        st.session_state.page = qp_page

    qp_job = qp.get("job_id", None)
    if qp_job:
        st.session_state.selected_job_id = qp_job

    qp_asset = qp.get("asset_id", None)
    if qp_asset:
        st.session_state.selected_asset_id = qp_asset


def do_logout_callback():
    try:
        st.query_params.clear()
    except Exception:
        pass

    for k in list(st.session_state.keys()):
        del st.session_state[k]
    init_state()
    request_rerun()
