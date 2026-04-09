import streamlit as st
from ui.pages.common.sidebar import sidebar
from ui.pages.common.home import page_home
from ui.pages.common.health import page_health
from ui.pages.common.data_quality import page_data_quality
from ui.pages.common.lineage import page_lineage
from ui.pages.common.field_lineage import page_field_lineage
from ui.pages.common.job_details import page_job_details
from ui.pages.common.asset_details import page_asset_details
from ui.pages.admin.rbac_matrix import page_rbac_matrix
from ui.pages.admin.org_levels import page_org_levels
from ui.pages.admin.openmetadata import page_openmetadata
from ui.pages.data_engineer.ingestion import page_ingestion
from ui.pages.data_engineer.kafka_ingestion import page_kafka_ingestion
from ui.pages.data_analyst.query_studio import page_query
from ui.pages.ml_engineer.features_models import page_ml
from ui.state import can, A_VIEW_RBAC, A_MANAGE_ORG, A_EDIT_METADATA
from ui.runtime import handle_deferred_rerun


def router():
    sidebar()

    page = st.session_state.page

    if page == "Home":
        page_home()
    elif page == "Health":
        page_health(embed=False)
    elif page == "Data Quality":
        page_data_quality()
    elif page == "Lineage":
        page_lineage()
    elif page == "Field Lineage":
        page_field_lineage()
    elif page == "RBAC Matrix":
        if can(A_VIEW_RBAC):
            page_rbac_matrix()
        else:
            st.warning("You do not have access to RBAC Matrix.")
            st.session_state.page = "Home"
    elif page == "Org Levels":
        if can(A_MANAGE_ORG):
            page_org_levels()
        else:
            st.warning("You do not have access to Org Levels.")
            st.session_state.page = "Home"
    elif page == "OpenMetadata":
        if can(A_EDIT_METADATA):
            page_openmetadata()
        else:
            st.warning("You do not have access to OpenMetadata.")
            st.session_state.page = "Home"
    elif page == "Ingestion":
        page_ingestion()
    elif page == "Kafka Ingestion":
        page_kafka_ingestion()
    elif page == "Query Studio":
        page_query()
    elif page == "Features & Models":
        page_ml()
    elif page == "Job Details":
        page_job_details()
    elif page == "Asset Details":
        page_asset_details()

    handle_deferred_rerun()
