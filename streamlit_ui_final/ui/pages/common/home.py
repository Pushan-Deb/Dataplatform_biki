import streamlit as st
from ui.services.jobs import ensure_dummy_jobs, visible_jobs_for_user, visible_assets_for_user
from ui.components.tables import jobs_table_with_open, assets_table_with_open


def page_home():
    st.title("Home")
    ensure_dummy_jobs()

    jobs = visible_jobs_for_user()
    assets = visible_assets_for_user()

    st.subheader("Jobs (searchable)")
    jobs_table_with_open(jobs, "Jobs", "home_jobs_search")

    st.divider()
    st.subheader("Published assets (searchable)")
    assets_table_with_open(assets, "Assets", "home_assets_search")
