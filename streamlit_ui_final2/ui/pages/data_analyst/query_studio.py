import streamlit as st
import pandas as pd
from ui.services.jobs import add_job
from ui.services.lineage import add_field_lineage
from ui.services.utils import with_run_ts
from ui.state import can, A_SUBMIT_QUERY, A_SET_VISIBILITY
from ui.components.chat import chat_form_split
from ui.config import TRINO_URL


def success_box(markdown_text: str):
    st.markdown(f"<div class='successbox'>{markdown_text}</div>", unsafe_allow_html=True)


def page_query():
    st.caption(f"Trino: {TRINO_URL}")

    def form():
        st.text_area("SQL", "SELECT * FROM sales.orders LIMIT 10", key="qs_sql")
        output_name = st.text_input("Result dataset name", "query_result_orders", key="qs_out_name")
        dest_base = f"s3://minio/gold/sales/mart/{output_name}/"

        st.dataframe(pd.DataFrame([{
            "Result Location (base)": dest_base,
            "Suggested unique path": with_run_ts(dest_base),
            "Execution": "Async job (mock)"
        }]), use_container_width=True, hide_index=True)

        job_vis = st.selectbox("Job visibility", ["Private", "Team", "Global"], disabled=not can(A_SET_VISIBILITY),
                               key="qs_job_vis")

        if st.button("Submit query job (mock)", type="primary", key="qs_submit", disabled=not can(A_SUBMIT_QUERY)):
            _, link, _ = add_job("SQL query", "Airflow + Trino", dest_base, status="QUEUED", visibility=job_vis,
                                 source="SQL", destination=dest_base)
            success_box(f"✅ Submitted (mock). &nbsp; <a href=\"{link}\">Open Job Details</a>")
            try:
                src_ds = st.session_state.openmetadata.get("dataset", "sales.orders")
                dest_ds = f"gold.sales.mart.{output_name}"
                cols = st.session_state.openmetadata.get("columns")
                if isinstance(cols, pd.DataFrame) and not cols.empty and "Column" in cols.columns:
                    for c in cols["Column"].tolist():
                        add_field_lineage(str(src_ds), str(c), dest_ds, str(c), "sql_select")
            except Exception:
                pass

    chat_form_split("Query Studio", "Example: Revenue by country last 30 days", form, context_key="query_studio")
