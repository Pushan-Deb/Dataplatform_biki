import streamlit as st
import pandas as pd
from ui.components.editors import kv_editor_table
from ui.runtime import request_rerun
from ui.config import OPENMETADATA_URL


def page_openmetadata():
    st.title("OpenMetadata – Dataset View (Mock)")
    st.caption(f"OpenMetadata UI: {OPENMETADATA_URL}")
    meta = st.session_state.openmetadata
    dataset = st.text_input("Dataset", value=meta.get("dataset", "sales.orders"), key="om_dataset")

    st.subheader("Columns (explicit meaning is editable)")
    cols_df = meta.get("columns").copy()
    meta["columns"] = st.data_editor(cols_df, use_container_width=True, hide_index=True, key="om_cols_editor")

    st.subheader("Dataset metadata")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Explicit Metadata (Editable)")
        meta["explicit"] = kv_editor_table("Explicit metadata", meta.get("explicit", {}), key="explicit_editor")
    with c2:
        st.markdown("#### Inferred Metadata (Read-only)")
        inferred_df = pd.DataFrame([{"Key": k, "Value": v} for k, v in meta.get("inferred", {}).items()])
        st.dataframe(inferred_df, use_container_width=True, hide_index=True, height=260)

    if st.button("Save Dataset Metadata", type="primary", key="om_save"):
        meta["dataset"] = dataset
        st.session_state.openmetadata = meta
        st.success("Saved (mock).")
        request_rerun()
