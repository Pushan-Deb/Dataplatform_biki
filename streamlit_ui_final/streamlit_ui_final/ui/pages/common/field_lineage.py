import streamlit as st
from ui.components.tables import show_table, search_box


def page_field_lineage():
    st.title("Field Lineage")
    df = st.session_state.field_lineage_table
    q = search_box("Search field lineage (dataset, column...)", key="field_lineage_search")
    view = df.copy()
    if q and not view.empty:
        mask = False
        for c in ["Source Dataset", "Source Column", "Target Dataset", "Target Column", "Transform", "Created At"]:
            mask = mask | view[c].astype(str).str.lower().str.contains(q)
        view = view[mask]
    show_table(view, "Column-level lineage (mock)")
