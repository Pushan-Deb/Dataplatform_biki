import streamlit as st
import pandas as pd
from ui.services.jobs import ensure_dummy_jobs, visible_jobs_for_user
from ui.services.dq import add_dq_run
from ui.services.jobs import publish_output_from_job
from ui.services.lineage import lineage_for_node
from ui.services.utils import dataset_id_from_s3_path
from ui.components.tables import jobs_table_with_open, html_table, show_table
from ui.components.serving import serving_tabs
from ui.state import can, A_PUBLISH_ASSET
from ui.config import AIRFLOW_URL, TRINO_URL, SPARK_UI, KAFKA_BOOTSTRAP
from ui.runtime import request_rerun


def page_job_details():
    st.title("Job Details")
    ensure_dummy_jobs()

    jobs = visible_jobs_for_user()
    job_id = st.session_state.selected_job_id

    if not job_id or (jobs["Job Id"] == job_id).sum() == 0:
        jobs_table_with_open(jobs, "Jobs", "jd_jobs_search")
        st.info("Use the Open link above to open a job.")
        return

    job = jobs[jobs["Job Id"] == job_id].iloc[0]
    st.dataframe(pd.DataFrame([job.to_dict()]), use_container_width=True, hide_index=True)

    st.info(
        "Advanced: In a real deployment, this Job Details would deep-link to the underlying OSS tool UIs (Airflow/Kafka/Trino). Below are real links.")
    adv_df = pd.DataFrame([
        {"Action": "Open in Airflow UI", "Link": AIRFLOW_URL},
        {"Action": "Open in Kafka UI", "Link": f"http://{KAFKA_BOOTSTRAP}"},
        {"Action": "Open in Trino UI", "Link": TRINO_URL},
        {"Action": "Open in Spark UI", "Link": SPARK_UI},
    ])
    adv_df["Action"] = adv_df.apply(lambda r: f"[{r['Action']}]({r['Link']})", axis=1)
    html_table(adv_df[["Action"]])

    st.markdown("### Lineage")
    show_table(lineage_for_node(job_id), "Edges involving this job")

    dest_ds = dataset_id_from_s3_path(str(job.get("Destination", "")))
    fld = st.session_state.field_lineage_table
    if dest_ds and not fld.empty:
        view = fld[fld["Target Dataset"].astype(str) == dest_ds]
        if view.empty:
            view = fld[fld["Target Dataset"].astype(str).str.contains(dest_ds.split(".")[-1], case=False, na=False)]
        if not view.empty:
            show_table(view, "Field lineage into this destination")

    serving_tabs(str(job.get("Result Location", "")), job_id)

    st.divider()
    st.markdown("### Data Quality checks (Great Expectations - mock)")
    suites = ["basic_suite", "null_checks", "pk_uniqueness"]
    suite = st.selectbox("Expectation Suite", suites, key="dq_suite")
    if st.button("Run DQ checks (mock)", key="dq_run_btn"):
        status = "FAILED" if suite == "null_checks" else "PASSED"
        failed = "amount_null_rate>0.01" if status == "FAILED" else ""
        add_dq_run("Job", job_id, suite, status, failed)
        st.success("DQ run recorded (mock).")
        request_rerun()

    dq = st.session_state.dq_runs_table
    if not dq.empty:
        recent = dq[dq["Related Id"] == job_id].sort_values("Created At", ascending=False).head(5)
        if not recent.empty:
            rows = []
            for _, r in recent.iterrows():
                rows.append({
                    "DQ Run Id": r["DQ Run Id"],
                    "Suite": r["Expectation Suite"],
                    "Status": r["Status"],
                    "Failed Checks": r["Failed Checks"],
                    "Open": f"[Open]({r['Open DQ Link']})",
                })
            html_table(pd.DataFrame(rows))

    st.divider()
    st.markdown("### Publish output (creates Published Asset)")
    c1, c2, c3 = st.columns(3)
    with c1:
        asset_type = st.selectbox("Type", ["Dataset", "Feature output", "Model artifact", "Report"], key="pub_type")
    with c2:
        asset_name = st.text_input("Name", f"{job.get('Job Type', 'Output')} - {job_id}", key="pub_name")
    with c3:
        visibility = st.selectbox("Visibility", ["Private", "Team", "Global"], key="pub_vis")

    if st.button("Publish", type="primary", key="pub_btn", disabled=not can(A_PUBLISH_ASSET)):
        publish_output_from_job(job_id, asset_type, asset_name, visibility)
        st.success("Published (mock).")
