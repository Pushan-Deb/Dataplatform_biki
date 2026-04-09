import streamlit as st
import pandas as pd
from ui.config import MINIO_URL


def render_download_tab(result_location: str, key_prefix: str):
    st.markdown("#### Download")
    st.dataframe(pd.DataFrame([{"Location": result_location,
                                 "How": f"Presigned URL from MinIO ({MINIO_URL})"}]),
                 use_container_width=True, hide_index=True)
