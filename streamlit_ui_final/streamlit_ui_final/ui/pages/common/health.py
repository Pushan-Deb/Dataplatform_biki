import streamlit as st
import requests
import pandas as pd
from ui.config import HEALTH_API, GRAFANA_KAFKA, GRAFANA_MINIO


def page_health(embed: bool = False):
    if not embed:
        st.title("Platform Health")

    try:
        resp = requests.get(HEALTH_API, timeout=5)
        data = resp.json()
        services = data.get("services", {})
    except Exception as e:
        st.error(f"Health API not reachable: {str(e)}")
        return

    ALL_TOOLS = {
        "kafka": "Event streaming backbone",
        "minio": "Object storage (S3-compatible)",
        "airflow": "Workflow orchestration",
        "spark": "Distributed processing engine",
        "trino": "SQL query engine",
        "keycloak": "Authentication & SSO",
        "vault": "Secrets management",
        "prometheus": "Metrics collection",
        "grafana": "Monitoring dashboards"
    }

    GRAFANA_DASHBOARDS = {
        "kafka": GRAFANA_KAFKA,
        "minio": GRAFANA_MINIO,
    }

    rows = []

    for tool_key, purpose in ALL_TOOLS.items():
        svc = services.get(tool_key, {})
        status = svc.get("status", "unknown").lower()

        if tool_key in GRAFANA_DASHBOARDS:
            if status == "healthy":
                color = "limegreen"
                label = "Healthy"
            elif status == "unhealthy":
                color = "crimson"
                label = "Unhealthy"
            else:
                color = "gray"
                label = "Unknown"

            grafana_url = GRAFANA_DASHBOARDS.get(tool_key)
            status_html = (
                f'<a href="{grafana_url}" target="_blank">'
                f'<span style="color:{color}; font-weight:700;">● {label}</span>'
                f'</a>'
            )
        else:
            status_html = '<span style="color:gray;">—</span>'

        rows.append({
            "Tool": tool_key.capitalize(),
            "Purpose": purpose,
            "Status": status_html
        })

    df = pd.DataFrame(rows)

    table_html = df.to_html(
        classes="health-table",
        escape=False,
        index=False
    )

    st.markdown("""
    <style>
    .health-table {
        width: 100%;
        border-collapse: collapse;
    }
    .health-table th, .health-table td {
        padding: 12px;
        text-align: left;
        border-bottom: 1px solid #ddd;
        font-size: 14px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(table_html, unsafe_allow_html=True)
