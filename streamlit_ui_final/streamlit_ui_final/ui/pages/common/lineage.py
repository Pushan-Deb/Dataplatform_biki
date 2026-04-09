import streamlit as st
from ui.components.tables import show_table, search_box


def page_lineage():
    st.title("Lineage")
    df = st.session_state.lineage_table
    q = search_box("Search lineage (job id, asset id, table, feature, model...)", key="lineage_search")
    view = df.copy()
    if q and not view.empty:
        mask = False
        for c in ["From Type", "From Id", "To Type", "To Id", "Relation", "Created At"]:
            mask = mask | view[c].astype(str).str.lower().str.contains(q)
        view = view[mask]
    show_table(view, "Lineage edges")
