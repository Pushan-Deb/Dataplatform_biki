import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from ui.services.jobs import add_job, visible_assets_for_user
from ui.services.features import latest_feature_values_rows
from ui.services.lineage import add_lineage_edge
from ui.state import can, A_TRAIN_MODELS, A_SET_VISIBILITY
from ui.components.chat import chat_form_split
from ui.components.tables import show_table


def success_box(markdown_text: str):
    st.markdown(f"<div class='successbox'>{markdown_text}</div>", unsafe_allow_html=True)


def render_models_tab():
    def form_models():
        model_name = st.text_input("Model Name", "churn_model", key="m_name")
        algo = st.selectbox("Algorithm", ["Logistic Regression", "XGBoost", "Random Forest"], key="m_algo")

        assets = visible_assets_for_user()
        dataset_assets = assets[assets["Type"] == "Dataset"] if not assets.empty else pd.DataFrame()
        if dataset_assets.empty:
            training_ds = st.text_input("Training dataset (table/path)", "gold.sales.mart.training_churn",
                                        key="m_train_ds_manual")
        else:
            training_ds = st.selectbox("Training dataset (published)", dataset_assets["Name"].tolist(),
                                       key="m_train_ds_pick")

        feature_options = st.session_state.features_table[
            "Feature Name"].tolist() if not st.session_state.features_table.empty else []
        st.caption(
            "Select feature definitions (X variables) used by this model. The platform will build training data by joining feature values with labels.")
        feature_set = st.multiselect("Feature Definitions to use", feature_options,
                                     default=feature_options[:1] if feature_options else [], key="m_feature_set")

        st.markdown("**Training data source (Feast)**")
        use_latest = st.checkbox("Use latest materialized Feature Values from Feast (offline)", value=True,
                                 key="m_use_latest")
        if use_latest and feature_set:
            reg = latest_feature_values_rows()
            resolved = []
            for f in feature_set:
                match = reg[reg["Feature Name"] == f]
                if not match.empty:
                    resolved.append({"Feature Name": f, "Offline Location": match.iloc[0]["Offline Location"],
                                     "Materialization Job Id": match.iloc[0]["Materialization Job Id"],
                                     "Status": match.iloc[0]["Status"]})
                else:
                    resolved.append({"Feature Name": f, "Offline Location": "Not materialized yet",
                                     "Materialization Job Id": "", "Status": "—"})
            show_table(pd.DataFrame(resolved), "Resolved feature value snapshots (latest)")

        if not feature_set:
            st.warning("Select at least one Feature Definition to proceed.")
        label_col = st.text_input("Label column", "churned", key="m_label")
        stage = st.selectbox("Stage", ["Draft", "Staging", "Production"], key="m_stage")

        if st.button("Submit model training job (mock)", type="primary", key="m_submit",
                     disabled=(not can(A_TRAIN_MODELS)) or (len(feature_set) == 0)):
            dest_base = f"s3://minio/models/{model_name}/"
            job_vis = "Team" if can(A_SET_VISIBILITY) else "Private"
            _, link, _ = add_job("Model training", "Airflow + Python", dest_base, status="QUEUED",
                                 visibility=job_vis, source=f"Model:{model_name}", destination=dest_base)

            run_id = str(uuid.uuid4())[:8]
            auc = 0.84
            row = {
                "Model Name": model_name,
                "Algorithm": algo,
                "Training Dataset": training_ds,
                "Feature Set": ", ".join(feature_set) if feature_set else "",
                "Label Column": label_col,
                "Stage": stage,
                "Run Id": run_id,
                "Metric (AUC)": auc,
                "Created At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            st.session_state.models_table = pd.concat([st.session_state.models_table, pd.DataFrame([row])],
                                                      ignore_index=True)
            add_lineage_edge("Dataset", training_ds, "Model", model_name, "trains_on")
            for f in feature_set:
                add_lineage_edge("Feature", f, "Model", model_name, "uses_feature")

            success_box(f"✅ Submitted (mock). &nbsp; <a href=\"{link}\">Open Job Details</a>")

        st.divider()
        show_table(st.session_state.models_table, "Models registry (mock)")

    chat_form_split("Models", "Example: Train model from curated training dataset.", form_models,
                    context_key="ml_models")
