import streamlit as st
from ui.components.serving_tabs.api import render_api_tab
from ui.components.serving_tabs.sftp import render_sftp_tab
from ui.components.serving_tabs.download import render_download_tab
from ui.components.serving_tabs.sample_preview import render_sample_preview_tab
from ui.components.tables import md_link


def serving_tabs(result_location: str, key_prefix: str):
    st.markdown("### Consume results (Serving options)")
    st.markdown(f"**Result location:** {md_link(result_location)}")
    tabs = st.tabs(["API", "SFTP", "Download", "Sample preview"])

    with tabs[0]:
        render_api_tab(result_location, key_prefix)

    with tabs[1]:
        render_sftp_tab(result_location, key_prefix)

    with tabs[2]:
        render_download_tab(result_location, key_prefix)

    with tabs[3]:
        render_sample_preview_tab(result_location, key_prefix)
