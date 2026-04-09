import streamlit as st
import pandas as pd
from ui.state import (ROLE_PERMS, can,
    A_VIEW_HEALTH, A_MANAGE_ORG, A_EDIT_METADATA, A_SUBMIT_INGESTION,
    A_SUBMIT_QUERY, A_MANAGE_FEATURES, A_TRAIN_MODELS, A_PUBLISH_ASSET, A_SET_VISIBILITY)
from ui.components.tables import show_table


def page_rbac_matrix():
    st.title("RBAC Matrix")
    actions = [
        (A_VIEW_HEALTH, "View platform health"),
        (A_MANAGE_ORG, "Manage org hierarchy"),
        (A_EDIT_METADATA, "Edit metadata (OpenMetadata mock)"),
        (A_SUBMIT_INGESTION, "Submit ingestion jobs"),
        (A_SUBMIT_QUERY, "Submit query jobs"),
        (A_MANAGE_FEATURES, "Create/edit features + materialize"),
        (A_TRAIN_MODELS, "Train models"),
        (A_PUBLISH_ASSET, "Publish outputs as assets"),
        (A_SET_VISIBILITY, "Set Team/Global visibility"),
    ]
    rows = []
    for role in ROLE_PERMS.keys():
        for a, desc in actions:
            rows.append({
                "Role": role,
                "Action": a,
                "Description": desc,
                "Allowed": "Yes" if a in ROLE_PERMS.get(role, set()) else "No"
            })
    show_table(pd.DataFrame(rows), "Role permissions")

    st.divider()
    st.markdown("### Current user permissions")
    my_rows = []
    for a, desc in actions:
        my_rows.append({"Action": a, "Description": desc, "Allowed": "Yes" if can(a) else "No"})
    show_table(pd.DataFrame(my_rows), "You")
