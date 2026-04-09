import streamlit as st
import pandas as pd
import re
from datetime import datetime


def detect_sql_sources(sql: str):
    """Best-effort SQL table reference extraction for FROM/JOIN clauses."""
    if not sql:
        return []
    s = re.sub(r"'[^']*'", "'',", sql)
    s = re.sub(r'"[^"]*"', '""', s)
    pats = re.findall(r"\b(from|join)\s+([a-zA-Z0-9_\.]+)", s, flags=re.IGNORECASE)
    out = []
    for _, tbl in pats:
        tbl = tbl.strip().strip(',').strip(';')
        if tbl and tbl.lower() not in ["select"]:
            out.append(tbl)
    seen = set()
    dedup = []
    for t in out:
        if t.lower() not in seen:
            seen.add(t.lower())
            dedup.append(t)
    return dedup


def get_feature_sources(feature_name: str) -> pd.DataFrame:
    df = st.session_state.get("feature_sources_table", pd.DataFrame())
    if df is None or df.empty:
        return pd.DataFrame(columns=["Dataset", "Confirmed", "Source", "Last Updated"])
    sub = df[df["Feature Name"] == feature_name].copy()
    if sub.empty:
        return pd.DataFrame(columns=["Dataset", "Confirmed", "Source", "Last Updated"])
    return sub[["Dataset", "Confirmed", "Source", "Last Updated"]].reset_index(drop=True)


def upsert_feature_sources(feature_name: str, rows_df: pd.DataFrame):
    """Replace all source rows for a feature definition."""
    base = st.session_state.get("feature_sources_table", pd.DataFrame(
        columns=["Feature Name", "Dataset", "Confirmed", "Source", "Last Updated"]))
    base = base[base["Feature Name"] != feature_name].copy()
    if rows_df is None:
        st.session_state.feature_sources_table = base
        return
    d = rows_df.copy()
    if d.empty:
        st.session_state.feature_sources_table = base
        return
    d["Feature Name"] = feature_name
    d["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "Confirmed" not in d.columns:
        d["Confirmed"] = False
    if "Source" not in d.columns:
        d["Source"] = "Manual"
    st.session_state.feature_sources_table = pd.concat(
        [base, d[["Feature Name", "Dataset", "Confirmed", "Source", "Last Updated"]]], ignore_index=True)


def add_feature_value_record(feature_name: str, entity: str, job_id: str, offline_location: str,
                              status: str = "MATERIALIZED", valid_from: str | None = None, valid_to: str = "",
                              store: str = "Feast (Offline)"):
    """Register a materialized feature-values snapshot (Feast-aligned)."""
    if valid_from is None:
        valid_from = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "Feature Name": feature_name,
        "Entity": entity,
        "Store": store,
        "Materialization Job Id": job_id,
        "Offline Location": offline_location,
        "Online Location": "",
        "Status": status,
        "Valid From": valid_from,
        "Valid To": valid_to,
        "Last Updated": now,
    }
    st.session_state.feature_values_registry = pd.concat(
        [st.session_state.feature_values_registry, pd.DataFrame([row])],
        ignore_index=True
    )


def latest_feature_values_rows(feature_name: str | None = None) -> pd.DataFrame:
    df = st.session_state.feature_values_registry
    if df is None or df.empty:
        return df
    view = df.copy()
    if feature_name:
        view = view[view["Feature Name"] == feature_name]
    view["__ts"] = pd.to_datetime(view["Last Updated"], errors="coerce")
    view = view.sort_values("__ts", ascending=False).drop(columns=["__ts"])
    return view


def ensure_dummy_feature_values_registry():
    if not st.session_state.feature_values_registry.empty:
        return
    add_feature_value_record(
        feature_name="customer_lifetime_value",
        entity="customer",
        job_id="JOB-DEMO-FEAT01",
        offline_location="s3://minio/feast/offline/customer/customer_lifetime_value/run_ts=20260130_115436/",
        status="MATERIALIZED",
        valid_from="2026-01-30 11:54:36"
    )
    add_feature_value_record(
        feature_name="num_orders_30d",
        entity="customer",
        job_id="JOB-DEMO-FEAT02",
        offline_location="s3://minio/feast/offline/customer/num_orders_30d/run_ts=20260130_120015/",
        status="MATERIALIZED",
        valid_from="2026-01-30 12:00:15"
    )
