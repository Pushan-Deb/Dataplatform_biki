import streamlit as st
import pandas as pd
from datetime import datetime
from ui.links import make_open_dq_link
from ui.services.lineage import add_lineage_edge


def add_dq_run(related_type: str, related_id: str, suite: str, status: str, failed_checks: str):
    dq_id = f"DQ-{__import__('uuid').uuid4().__str__()[:8]}"
    created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    link = make_open_dq_link(dq_id)
    row = {
        "DQ Run Id": dq_id,
        "Related Type": related_type,
        "Related Id": related_id,
        "Expectation Suite": suite,
        "Status": status,
        "Failed Checks": failed_checks,
        "Created At": created,
        "Open DQ Link": link,
    }
    st.session_state.dq_runs_table = pd.concat([st.session_state.dq_runs_table, pd.DataFrame([row])],
                                                ignore_index=True)
    add_lineage_edge("DQ Run", dq_id, related_type, related_id, "validates")
    return dq_id


def ensure_dummy_dq():
    if not st.session_state.dq_runs_table.empty:
        return
    add_dq_run("Dataset", "gold.sales.customer_agg", "basic_suite", "PASSED", "")
    add_dq_run("Dataset", "bronze.sales.orders", "null_checks", "FAILED", "amount_null_rate>0.01")


def latest_dq_for_job(job_id: str):
    """Return (status, failed_checks, suite, created_at) for latest DQ run for a job."""
    df = st.session_state.get("dq_runs_table")
    if df is None or df.empty:
        return (None, None, None, None)
    d = df[(df["Related Type"] == "Job") & (df["Related Id"] == job_id)]
    if d.empty:
        return (None, None, None, None)
    d = d.sort_values("Created At", ascending=False)
    r = d.iloc[0]
    return (str(r.get("Status", "")), str(r.get("Failed Checks", "")), str(r.get("Expectation Suite", "")),
            str(r.get("Created At", "")))


def dq_badge_html(status: str | None):
    """Small colored badge for DQ status."""
    if not status:
        return "<span style='padding:2px 8px;border-radius:999px;background:#444;color:#fff;font-size:11px;'>No DQ</span>"
    s = status.upper()
    if s in ["PASSED", "SUCCESS", "SUCCEEDED"]:
        bg = "#1f8b4c"
        txt = "PASSED"
    elif s in ["FAILED", "FAIL"]:
        bg = "#b42318"
        txt = "FAILED"
    elif s in ["RUNNING"]:
        bg = "#b54708"
        txt = "RUNNING"
    else:
        bg = "#444"
        txt = s
    return f"<span style='padding:2px 8px;border-radius:999px;background:{bg};color:#fff;font-size:11px;font-weight:600;'>{txt}</span>"
