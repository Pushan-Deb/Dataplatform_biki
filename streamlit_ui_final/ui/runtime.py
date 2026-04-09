import streamlit as st


def rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()


def request_rerun():
    st.session_state.request_rerun = True


def handle_deferred_rerun():
    if st.session_state.get("request_rerun", False):
        st.session_state.request_rerun = False
        rerun()
