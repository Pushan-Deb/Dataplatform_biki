import streamlit as st
import pandas as pd


def render_sample_preview_tab(result_location: str, key_prefix: str):
    st.markdown("#### Sample preview")
    st.dataframe(pd.DataFrame(
        [{"col1": "value1", "col2": 10}, {"col1": "value2", "col2": 20}, {"col1": "value3", "col2": 30}]),
                 use_container_width=True, hide_index=True)
