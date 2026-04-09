import streamlit as st
import pandas as pd
from ui.config import KEYCLOAK_SERVER


def render_api_tab(result_location: str, key_prefix: str):
    st.markdown("#### API")
    auth_df = pd.DataFrame([
        {"Auth option": "Keycloak OAuth2 (recommended)", "When to use": "User-facing APIs"},
        {"Auth option": "Signed JWT (service-to-service)", "When to use": "Internal services"},
        {"Auth option": "API Key (least preferred)", "When to use": "Simple integrations; rotate keys"},
        {"Auth option": "mTLS", "When to use": "High-trust internal networks"},
    ])
    st.dataframe(auth_df, use_container_width=True, hide_index=True)
    st.caption(f"Keycloak SSO: {KEYCLOAK_SERVER}")
    endpoint = st.text_input("Endpoint route (mock)", f"/v1/results/{key_prefix}", key=f"api_route_{key_prefix}")
    auth = st.selectbox("Auth", auth_df["Auth option"].tolist(), key=f"api_auth_{key_prefix}")
    st.dataframe(pd.DataFrame([{"Endpoint": endpoint, "Auth": auth, "Backed by": result_location}]),
                 use_container_width=True, hide_index=True)
