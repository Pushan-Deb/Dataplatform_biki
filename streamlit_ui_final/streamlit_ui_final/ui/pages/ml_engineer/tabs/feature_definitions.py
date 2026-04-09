import streamlit as st
import pandas as pd
from datetime import datetime
from ui.services.features import (detect_sql_sources, get_feature_sources, upsert_feature_sources,
                                   add_feature_value_record, latest_feature_values_rows)
from ui.services.jobs import add_job
from ui.services.lineage import add_lineage_edge
from ui.services.utils import with_run_ts
from ui.state import can, A_MANAGE_FEATURES, A_SET_VISIBILITY
from ui.components.chat import chat_form_split
from ui.components.tables import show_table
from ui.config import SPARK_MASTER, SPARK_UI


def success_box(markdown_text: str):
    st.markdown(f"<div class='successbox'>{markdown_text}</div>", unsafe_allow_html=True)


def render_feature_definitions_tab():
    st.caption(f"Spark Master: {SPARK_MASTER} | Spark UI: {SPARK_UI}")

    def form_features():
        feature_name = st.text_input("Feature Definition Name", "customer_lifetime_value", key="f_name")
        entity = st.selectbox("Entity", ["customer", "account", "device"], key="f_entity")
        sql = st.text_area("Feature SQL / Logic",
                           "SELECT customer_id, SUM(revenue) AS clv FROM ... GROUP BY customer_id", key="f_sql")

        st.markdown("**Source datasets (auto-detected from SQL)**")
        detected = detect_sql_sources(sql)
        existing = get_feature_sources(feature_name)
        if existing.empty:
            src_df = pd.DataFrame(
                [{"Dataset": d, "Confirmed": False, "Source": "Detected", "Last Updated": ""} for d in detected])
            if src_df.empty:
                src_df = pd.DataFrame(columns=["Dataset", "Confirmed", "Source", "Last Updated"])
        else:
            src_df = existing.copy()

        c_src1, c_src2 = st.columns([1, 1])
        with c_src1:
            if st.button("Re-detect from SQL", key="f_redetect"):
                src_df = pd.DataFrame(
                    [{"Dataset": d, "Confirmed": False, "Source": "Detected", "Last Updated": ""} for d in
                     detect_sql_sources(sql)])
                st.session_state["f_sources_editor"] = src_df
        with c_src2:
            st.caption("You can edit the detected list. Confirmed = reviewed by owner.")

        editor_df = st.data_editor(
            st.session_state.get("f_sources_editor", src_df),
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            key="f_sources_editor",
        )
        description = st.text_input("Description", "Total revenue per customer (proxy for CLV).", key="f_desc")
        window = st.selectbox("Window", ["7d", "30d", "90d", "180d", "All-time"], index=2, key="f_window")
        cadence = st.selectbox("Refresh Cadence", ["On-demand", "Hourly", "Daily", "Weekly"], index=2,
                               key="f_cadence")
        owner = st.text_input("Owner", st.session_state.get("user", ""), key="f_owner")

        if st.button("Save / Update feature definition", type="primary", key="f_save",
                     disabled=not can(A_MANAGE_FEATURES)):
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df = st.session_state.features_table
            if not df.empty and (df["Feature Name"] == feature_name).any():
                idx = df.index[df["Feature Name"] == feature_name][0]
                src_list = [str(x) for x in editor_df.get("Dataset", []) if str(x).strip()]
                src_display = ", ".join(src_list) if src_list else ""
                df.loc[idx, ["Entity", "Source Table", "Definition SQL", "Description", "Window", "Refresh Cadence",
                             "Owner", "Created At"]] = [entity, src_display, sql, description, window, cadence,
                                                        owner, now]
                st.session_state.features_table = df
            else:
                src_list = [str(x) for x in editor_df.get("Dataset", []) if str(x).strip()]
                src_display = ", ".join(src_list) if src_list else ""
                row = {"Feature Name": feature_name, "Entity": entity, "Source Table": src_display,
                       "Definition SQL": sql, "Description": description, "Window": window,
                       "Refresh Cadence": cadence, "Owner": owner, "Version": "v1", "Stage": "Draft",
                       "Created At": now}
                st.session_state.features_table = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            upsert_feature_sources(feature_name, editor_df)
            for ds in [str(x) for x in editor_df.get("Dataset", []) if str(x).strip()]:
                add_lineage_edge("Dataset", ds, "Feature", feature_name, "defines")
            st.success("Saved (mock).")

        st.markdown("#### Materialize Feature Values (Offline)")
        st.dataframe(pd.DataFrame([{
            "What happens": "A job computes feature values per entity and writes to MinIO (offline store).",
            "Output (base path)": f"s3://minio/gold/features/{entity}/{feature_name}/",
            "Suggested unique path": with_run_ts(f"s3://minio/gold/features/{entity}/{feature_name}/"),
        }]), use_container_width=True, hide_index=True)

        if st.button("Submit materialization job (mock)", type="secondary", key="f_submit_job",
                     disabled=not can(A_MANAGE_FEATURES)):
            dest_base = f"s3://minio/gold/features/{entity}/{feature_name}/"
            job_vis = "Team" if can(A_SET_VISIBILITY) else "Private"
            job_id, link, result_loc = add_job("Feature materialization", "Airflow + Spark", dest_base,
                                               status="QUEUED", visibility=job_vis,
                                               source=f"Feature:{feature_name}", destination=dest_base)
            add_feature_value_record(feature_name=feature_name, entity=entity, job_id=job_id,
                                     offline_location=str(result_loc))
            success_box(f"✅ Submitted (mock). &nbsp; <a href=\"{link}\">Open Job Details</a>")

        st.divider()
        show_table(st.session_state.features_table.drop(columns=["Source Table"], errors="ignore"),
                   "Saved features")
        st.divider()
        st.markdown("#### Feature Values Registry (Feast – Offline)")
        reg = latest_feature_values_rows(feature_name)
        if reg is None or reg.empty:
            st.info("No materialized feature values yet. Submit a materialization job to create a registry entry.")
        else:
            show_table(reg[["Feature Name", "Entity", "Store", "Materialization Job Id", "Offline Location",
                            "Status", "Valid From", "Last Updated"]], "Latest materializations for this feature")

        st.markdown("##### All Feature Values (latest first)")
        show_table(latest_feature_values_rows(), "Feature values registry")

    chat_form_split("Feature Definitions",
                    "Example: Define feature logic (X variable) and materialize values for reuse.", form_features,
                    context_key="ml_features")
