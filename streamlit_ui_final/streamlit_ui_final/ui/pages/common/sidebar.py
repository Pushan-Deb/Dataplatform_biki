import streamlit as st
from datetime import datetime
from ui.auth import do_logout_callback


def sidebar():
    with st.sidebar:
        st.write(f"👤 **{st.session_state.user}**")
        st.write(f"Role: **{st.session_state.role}**")
        st.write(f"Team: **{st.session_state.team}**")
        col_a, col_b = st.columns([1, 1])
        with col_a:
            st.button("Logout", on_click=do_logout_callback, key="logout_btn")
        with col_b:
            st.caption(datetime.now().strftime("%Y-%m-%d %H:%M"))
        st.divider()

        pages = ["Home", "Health", "Data Quality", "Lineage", "Field Lineage", "Job Details", "Asset Details"]
        if st.session_state.role == "Admin":
            pages += ["Org Levels", "OpenMetadata", "RBAC Matrix"]
        if st.session_state.role == "Data Engineer":
            pages += ["Ingestion", "Kafka Ingestion"]
        if st.session_state.role == "Data Analyst":
            pages += ["Query Studio"]
        if st.session_state.role == "ML Engineer":
            pages += ["Features & Models"]

        choice = st.radio("Navigate", pages,
                          index=pages.index(st.session_state.page) if st.session_state.page in pages else 0,
                          key="nav_radio")
        st.session_state.page = choice
