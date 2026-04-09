import streamlit as st
import pandas as pd
from datetime import datetime

from ui.config import ROLE_TEAMS

# Actions (platform-level)
A_VIEW_HEALTH = "view_health"
A_MANAGE_ORG = "manage_org_hierarchy"
A_EDIT_METADATA = "edit_metadata"
A_SUBMIT_INGESTION = "submit_ingestion"
A_SUBMIT_QUERY = "submit_query"
A_MANAGE_FEATURES = "manage_features"
A_TRAIN_MODELS = "train_models"
A_PUBLISH_ASSET = "publish_asset"
A_SET_VISIBILITY = "set_visibility"
A_VIEW_RBAC = "view_rbac_matrix"

ROLE_PERMS = {
    "Admin": {A_VIEW_HEALTH, A_MANAGE_ORG, A_EDIT_METADATA, A_SUBMIT_INGESTION, A_SUBMIT_QUERY,
              A_MANAGE_FEATURES, A_TRAIN_MODELS, A_PUBLISH_ASSET, A_SET_VISIBILITY, A_VIEW_RBAC},
    "Data Engineer": {A_VIEW_HEALTH, A_SUBMIT_INGESTION, A_PUBLISH_ASSET, A_SET_VISIBILITY},
    "Data Analyst": {A_VIEW_HEALTH, A_SUBMIT_QUERY, A_PUBLISH_ASSET},
    "ML Engineer": {A_VIEW_HEALTH, A_MANAGE_FEATURES, A_TRAIN_MODELS, A_PUBLISH_ASSET, A_SET_VISIBILITY},
}


def can(action: str) -> bool:
    role = st.session_state.get("role", None)
    if not role:
        return False
    return action in ROLE_PERMS.get(role, set())


def enforce(action: str):
    """Stop the current run if the user is not allowed to perform `action`."""
    if not can(action):
        role = st.session_state.get('role', 'Unknown')
        st.error(f"Not authorized: role '{role}' cannot perform '{action}'.")
        st.stop()


def init_state():
    defaults = {
        "logged_in": False,
        "role": None,
        "user": None,
        "team": None,
        "page": "Home",
        "request_rerun": False,

        "chat_history": [],
        "chat_draft_by_context": {},

        "org_hierarchy": {
            "Bronze": {"sales": ["orders", "customers", "events"], "finance": ["payments", "invoices"],
                       "crm": ["contacts", "accounts"]},
            "Silver": {"sales": ["orders_clean", "customers_clean"], "finance": ["payments_clean"],
                       "crm": ["contacts_clean"]},
            "Gold": {"sales": ["customer_agg", "revenue_mart", "features"], "finance": ["finance_mart"],
                     "crm": ["crm_mart"]},
        },

        "openmetadata": {
            "dataset": "sales.orders",
            "columns": pd.DataFrame([
                {"Column": "order_id", "Type": "INT", "Explicit meaning": "Primary key",
                 "Inferred meaning": "Unique order identifier"},
                {"Column": "customer_id", "Type": "STRING", "Explicit meaning": "FK to customers",
                 "Inferred meaning": "Join key to customers.id"},
                {"Column": "order_ts", "Type": "TIMESTAMP", "Explicit meaning": "Order time",
                 "Inferred meaning": "Event timestamp for ordering"},
                {"Column": "amount", "Type": "DOUBLE", "Explicit meaning": "Order amount",
                 "Inferred meaning": "Revenue metric"},
            ]),
            "explicit": {
                "Display Name": "Customer Orders",
                "Description": "Confirmed business description (editable).",
                "Primary Key": "order_id",
                "Owner": "Sales Team",
            },
            "inferred": {
                "Display Name": "Orders (inferred)",
                "Primary Key": "order_id",
                "Description": "Orders table with transactional data",
                "Join Hint": "orders.customer_id → customers.id",
                "Confidence": "0.72",
            }
        },

        "features_table": pd.DataFrame(
            columns=["Feature Name", "Entity", "Source Table", "Definition SQL", "Description", "Window",
                     "Refresh Cadence", "Owner", "Version", "Stage", "Created At"]),
        "feature_sources_table": pd.DataFrame(
            columns=["Feature Name", "Dataset", "Confirmed", "Source", "Last Updated"]),
        "feature_values_registry": pd.DataFrame(
            columns=["Feature Name", "Entity", "Store", "Materialization Job Id", "Offline Location", "Online Location",
                     "Status", "Valid From", "Valid To", "Last Updated"]),
        "models_table": pd.DataFrame(columns=[
            "Model Name", "Algorithm", "Training Dataset", "Feature Set", "Label Column", "Stage", "Run Id",
            "Metric (AUC)", "Created At"
        ]),

        "jobs_table": pd.DataFrame(columns=[
            "Job Id", "Submitted By", "Role", "Team", "Visibility", "Job Type", "Orchestrator", "Status", "Created At",
            "Source", "Destination", "Result Location", "Open Job Link"
        ]),
        "published_assets": pd.DataFrame(columns=[
            "Asset Id", "Type", "Name", "Visibility", "Owner", "Team", "Published At", "Source Job Id",
            "Result Location", "Open Asset Link"
        ]),

        "lineage_table": pd.DataFrame(columns=[
            "Edge Id", "From Type", "From Id", "To Type", "To Id", "Relation", "Created At"
        ]),

        "field_lineage_table": pd.DataFrame(columns=[
            "Edge Id", "Source Dataset", "Source Column", "Target Dataset", "Target Column", "Transform", "Created At"
        ]),

        "dq_runs_table": pd.DataFrame(columns=[
            "DQ Run Id", "Related Type", "Related Id", "Expectation Suite", "Status", "Failed Checks", "Created At",
            "Open DQ Link"
        ]),

        "selected_job_id": None,
        "selected_asset_id": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
