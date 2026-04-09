import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from ui.links import make_open_job_link, make_open_asset_link
from ui.services.utils import with_run_ts
from ui.services.lineage import add_lineage_edge, ensure_dummy_field_lineage, ensure_dummy_lineage
from ui.services.features import ensure_dummy_feature_values_registry
from ui.runtime import request_rerun


def add_job(job_type: str, orchestrator: str, result_location_base: str, submitted_by: str | None = None,
            role: str | None = None, status: str = "QUEUED", visibility: str = "Private", team: str | None = None,
            source: str | None = None, destination: str | None = None):
    job_id = f"JOB-{str(uuid.uuid4())[:8]}"
    created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result_location = with_run_ts(result_location_base)
    open_link = make_open_job_link(job_id)
    row = {
        "Job Id": job_id,
        "Submitted By": submitted_by if submitted_by else st.session_state.user,
        "Role": role if role else st.session_state.role,
        "Team": team if team else st.session_state.team,
        "Visibility": visibility,
        "Job Type": job_type,
        "Orchestrator": orchestrator,
        "Status": status,
        "Created At": created,
        "Source": source if source else "",
        "Destination": destination if destination else "",
        "Result Location": result_location,
        "Open Job Link": open_link,
    }
    st.session_state.jobs_table = pd.concat([st.session_state.jobs_table, pd.DataFrame([row])], ignore_index=True)
    st.session_state.selected_job_id = job_id
    if source:
        add_lineage_edge("Input", source, "Job", job_id, "feeds")
    if destination:
        add_lineage_edge("Job", job_id, "Output", destination, "writes")
    add_lineage_edge("Job", job_id, "Location", result_location, "stores_at")
    return job_id, open_link, result_location


def ensure_dummy_assets():
    if not st.session_state.published_assets.empty:
        return
    user = st.session_state.user or "Admin"
    team = st.session_state.team or "Platform"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    a1 = "AST-" + str(uuid.uuid4())[:8]
    a2 = "AST-" + str(uuid.uuid4())[:8]
    st.session_state.published_assets = pd.concat([st.session_state.published_assets, pd.DataFrame([
        {"Asset Id": a1, "Type": "Dataset", "Name": "gold.sales.customer_agg", "Visibility": "Global", "Owner": user,
         "Team": team, "Published At": now, "Source Job Id": "JOB-DEMO-0001",
         "Result Location": "s3://minio/gold/sales/customer_agg/run_ts=20260131_101500/",
         "Open Asset Link": make_open_asset_link(a1)},
        {"Asset Id": a2, "Type": "Report", "Name": "Daily Revenue Report", "Visibility": "Team", "Owner": user,
         "Team": team, "Published At": now, "Source Job Id": "JOB-DEMO-0002",
         "Result Location": "s3://minio/reports/revenue/run_ts=20260131_090000/",
         "Open Asset Link": make_open_asset_link(a2)},
    ])], ignore_index=True)


def ensure_dummy_jobs():
    ensure_dummy_field_lineage()
    ensure_dummy_lineage()
    ensure_dummy_assets()
    ensure_dummy_feature_values_registry()
    if not st.session_state.jobs_table.empty:
        return
    add_job("Ingestion", "Airbyte + Airflow", "s3://minio/bronze/sales/orders/", submitted_by="Data Engineer",
            role="Data Engineer", status="SUCCEEDED")
    add_job("SQL query", "Airflow + Trino", "s3://minio/gold/sales/mart/query_orders/", submitted_by="Data Analyst",
            role="Data Analyst", status="RUNNING")
    add_job("Feature materialization", "Airflow + Spark", "s3://minio/gold/sales/features/customer_lifetime_value/",
            submitted_by="ML Engineer", role="ML Engineer", status="SUCCEEDED")
    add_job("Model training", "Airflow + Python", "s3://minio/models/churn_model/", submitted_by="ML Engineer",
            role="ML Engineer", status="FAILED")


def visible_jobs_for_user():
    jobs = st.session_state.jobs_table
    if jobs is None or jobs.empty:
        return jobs
    role = st.session_state.role
    user = st.session_state.user
    team = st.session_state.get("team", None)
    if role == "Admin":
        return jobs
    global_jobs = jobs[jobs["Visibility"] == "Global"]
    team_jobs = jobs[(jobs["Visibility"] == "Team") & (jobs["Team"] == team)]
    private_jobs = jobs[(jobs["Visibility"] == "Private") & (jobs["Submitted By"] == user)]
    return pd.concat([global_jobs, team_jobs, private_jobs], ignore_index=True)


def visible_assets_for_user():
    assets = st.session_state.published_assets
    if assets.empty:
        return assets
    role = st.session_state.role
    user = st.session_state.user
    team = st.session_state.team
    if role == "Admin":
        return assets
    global_assets = assets[assets["Visibility"] == "Global"]
    team_assets = assets[(assets["Visibility"] == "Team") & (assets["Team"] == team)]
    private_assets = assets[(assets["Visibility"] == "Private") & (assets["Owner"] == user)]
    return pd.concat([global_assets, team_assets, private_assets], ignore_index=True)


def publish_output_from_job(job_id: str, asset_type: str, name: str, visibility: str):
    assets = st.session_state.published_assets
    jobs = visible_jobs_for_user()
    job = jobs[jobs["Job Id"] == job_id].iloc[0]
    asset_id = f"AST-{str(uuid.uuid4())[:8]}"
    published_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    open_link = make_open_asset_link(asset_id)
    row = {
        "Asset Id": asset_id,
        "Type": asset_type,
        "Name": name,
        "Visibility": visibility,
        "Owner": st.session_state.user,
        "Team": st.session_state.team,
        "Published At": published_at,
        "Source Job Id": job_id,
        "Result Location": str(job.get("Result Location", "")),
        "Open Asset Link": open_link,
    }
    st.session_state.published_assets = pd.concat([assets, pd.DataFrame([row])], ignore_index=True)
    add_lineage_edge("Job", job_id, "Asset", asset_id, "publishes")
    st.session_state.selected_asset_id = asset_id
    st.session_state.page = "Asset Details"
    request_rerun()
