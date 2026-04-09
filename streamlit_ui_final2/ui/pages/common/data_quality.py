import streamlit as st
import pandas as pd
from ui.services.jobs import ensure_dummy_jobs
from ui.services.dq import ensure_dummy_dq
from ui.components.tables import html_table, search_box


def page_data_quality():
    st.title("Data Quality (Great Expectations - mock)")
    ensure_dummy_jobs()
    ensure_dummy_dq()

    q = search_box("Search DQ runs", key="dq_search")
    df = st.session_state.dq_runs_table.copy()
    if q:
        mask = False
        for c in ["DQ Run Id", "Related Type", "Related Id", "Expectation Suite", "Status", "Failed Checks",
                  "Created At"]:
            mask = mask | df[c].astype(str).str.lower().str.contains(q)
        df = df[mask]
    if df.empty:
        st.info("No matching DQ runs.")
        return

    rows = []
    for _, r in df.sort_values("Created At", ascending=False).iterrows():
        rows.append({
            "DQ Run Id": r["DQ Run Id"],
            "Related": f"{r['Related Type']} / {r['Related Id']}",
            "Suite": r["Expectation Suite"],
            "Status": r["Status"],
            "Failed Checks": r["Failed Checks"],
            "Created At": r["Created At"],
            "Open": f"[Open]({r['Open DQ Link']})",
        })
    html_table(pd.DataFrame(rows))
