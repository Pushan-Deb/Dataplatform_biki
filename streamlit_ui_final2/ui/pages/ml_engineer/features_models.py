import streamlit as st
from ui.services.jobs import ensure_dummy_jobs
from ui.pages.ml_engineer.tabs.feature_definitions import render_feature_definitions_tab
from ui.pages.ml_engineer.tabs.models import render_models_tab


def page_ml():
    st.title("ML – Features & Models")
    st.caption(
        "Features are model input variables (X). Define feature logic once, materialize values via jobs, and reuse across models.")
    ensure_dummy_jobs()
    tabs = st.tabs(["Feature Definitions", "Models"])

    with tabs[0]:
        render_feature_definitions_tab()

    with tabs[1]:
        render_models_tab()
