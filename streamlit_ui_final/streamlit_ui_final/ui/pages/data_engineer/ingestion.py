import streamlit as st
import pandas as pd
from ui.services.jobs import add_job
from ui.services.lineage import add_field_lineage
from ui.services.utils import with_run_ts
from ui.state import can, A_SUBMIT_INGESTION, A_SET_VISIBILITY
from ui.components.chat import chat_form_split
from ui.config import AIRBYTE_UI, AIRBYTE_API


def success_box(markdown_text: str):
    st.markdown(f"<div class='successbox'>{markdown_text}</div>", unsafe_allow_html=True)


def page_ingestion():
    st.caption(f"Airbyte UI: {AIRBYTE_UI} | Airbyte API: {AIRBYTE_API}")
    hierarchy = st.session_state.org_hierarchy
    mock_sources = {"crm-postgres": {"objects": ["public.customers", "public.orders", "public.events"]},
                    "files-sftp": {"objects": ["customers.csv", "orders.csv", "events.jsonl"]}}

    def form():
        source = st.selectbox("Source Connector", list(mock_sources.keys()), key="ing_source")
        source_object = st.selectbox("Source table / file", mock_sources[source]["objects"], key="ing_source_obj")

        l1 = st.selectbox("Level 1", list(hierarchy.keys()), key="ing_l1")
        l2 = st.selectbox("Level 2", list(hierarchy.get(l1, {}).keys()), key="ing_l2")
        level3s = hierarchy.get(l1, {}).get(l2, [])
        l3 = st.selectbox("Level 3", level3s if level3s else [""], key="ing_l3")

        dest_dataset = st.text_input("Destination dataset name",
                                     value=source_object.split(".")[-1].replace(".csv", "").replace(".jsonl", ""),
                                     key="ing_dest_dataset")

        base_prefix = f"s3://minio/{l1.lower()}/{l2}"
        if str(l3).strip():
            base_prefix += f"/{l3}"
        dest_base = f"{base_prefix}/{dest_dataset}/"

        st.dataframe(pd.DataFrame([{
            "Destination Path (base)": dest_base,
            "Suggested unique path": with_run_ts(dest_base)
        }]), use_container_width=True, hide_index=True)

        job_vis = st.selectbox("Job visibility", ["Private", "Team", "Global"], disabled=not can(A_SET_VISIBILITY),
                               key="ing_job_vis")

        if st.button("Submit ingestion job (mock)", type="primary", key="ing_submit",
                     disabled=not can(A_SUBMIT_INGESTION)):
            _, link, _ = add_job("Ingestion", "Airbyte + Airflow", dest_base, status="QUEUED", visibility=job_vis,
                                 source=source_object, destination=dest_base)
            success_box(f"✅ Submitted (mock). &nbsp; <a href=\"{link}\">Open Job Details</a>")
            try:
                dest_ds_id = f"{l1.lower()}.{l2}.{dest_dataset}"
                cols = st.session_state.openmetadata.get("columns")
                if isinstance(cols, pd.DataFrame) and not cols.empty and "Column" in cols.columns:
                    for c in cols["Column"].tolist():
                        add_field_lineage(str(source_object), str(c), dest_ds_id, str(c), "copy")
            except Exception:
                pass

    chat_form_split("Data Ingestion", "Example: Ingest orders into Bronze → sales → orders.", form,
                    context_key="ingestion")
