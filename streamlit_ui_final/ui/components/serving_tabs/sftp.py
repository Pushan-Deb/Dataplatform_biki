import streamlit as st
import pandas as pd


def render_sftp_tab(result_location: str, key_prefix: str):
    st.markdown("#### SFTP")
    st.dataframe(pd.DataFrame([{"SFTP path": f"/exports/{key_prefix}/", "Source": result_location}]),
                 use_container_width=True, hide_index=True)
