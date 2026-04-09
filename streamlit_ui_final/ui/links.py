import streamlit as st
import urllib.parse
from urllib.parse import unquote_plus, parse_qs
from ui.runtime import request_rerun


def go_to_link(link: str):
    """Navigate using internal query-param links like '?page=Job%20Details&job_id=...'"""
    if not link:
        return
    try:
        qs = link[1:] if link.startswith("?") else link
        parsed = parse_qs(qs, keep_blank_values=True)

        try:
            st.query_params.clear()
        except Exception:
            pass
        for k, v in parsed.items():
            if v:
                st.query_params[k] = v[0]

        if "page" in parsed and parsed["page"]:
            st.session_state.page = unquote_plus(parsed["page"][0])
        if "job_id" in parsed and parsed["job_id"]:
            st.session_state.selected_job_id = parsed["job_id"][0]
        if "asset_id" in parsed and parsed["asset_id"]:
            st.session_state.selected_asset_id = parsed["asset_id"][0]

        if "user" in parsed and "role" in parsed and parsed["user"] and parsed["role"]:
            st.session_state.logged_in = True
            st.session_state.user = unquote_plus(parsed["user"][0])
            st.session_state.role = unquote_plus(parsed["role"][0])
        if "team" in parsed and parsed["team"]:
            st.session_state.team = unquote_plus(parsed["team"][0])

        request_rerun()
    except Exception:
        return


def make_open_job_link(job_id: str):
    user = st.session_state.user or ""
    role = st.session_state.role or ""
    team = st.session_state.get("team", "") or ""
    u = urllib.parse.quote_plus(user)
    r = urllib.parse.quote_plus(role)
    t = urllib.parse.quote_plus(team)
    return f"?page=Job%20Details&job_id={job_id}&user={u}&role={r}&team={t}"


def make_open_asset_link(asset_id: str):
    user = st.session_state.user or ""
    role = st.session_state.role or ""
    team = st.session_state.get("team", "") or ""
    u = urllib.parse.quote_plus(user)
    r = urllib.parse.quote_plus(role)
    t = urllib.parse.quote_plus(team)
    return f"?page=Asset%20Details&asset_id={asset_id}&user={u}&role={r}&team={t}"


def make_open_dq_link(dq_run_id: str):
    user = st.session_state.user or ""
    role = st.session_state.role or ""
    team = st.session_state.team or ""
    return f"?page=Data%20Quality&dq_run_id={urllib.parse.quote_plus(dq_run_id)}&user={urllib.parse.quote_plus(user)}&role={urllib.parse.quote_plus(role)}&team={urllib.parse.quote_plus(team)}"
