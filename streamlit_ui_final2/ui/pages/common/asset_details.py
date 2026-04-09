import streamlit as st
import pandas as pd
from ui.services.jobs import visible_assets_for_user
from ui.services.lineage import lineage_for_node
from ui.services.utils import dataset_id_from_s3_path
from ui.components.tables import assets_table_with_open, show_table
from ui.components.serving import serving_tabs


def page_asset_details():
    st.title("Asset Details")

    assets_all = st.session_state.published_assets
    if assets_all.empty:
        st.info("No published assets yet. Publish from Job Details.")
        return

    assets_visible = visible_assets_for_user()
    asset_id = st.session_state.selected_asset_id

    if not asset_id or (assets_visible["Asset Id"] == asset_id).sum() == 0:
        assets_table_with_open(assets_visible, "Published assets you can access", "ad_assets_search")
        st.info("Use the Open link above to open an asset.")
        return

    asset = assets_all[assets_all["Asset Id"] == asset_id].iloc[0]
    st.dataframe(pd.DataFrame([asset.to_dict()]), use_container_width=True, hide_index=True)

    st.markdown("### Lineage")
    show_table(lineage_for_node(asset_id), "Edges involving this asset")

    dest_ds = dataset_id_from_s3_path(str(asset.get("Result Location", "")))
    fld = st.session_state.field_lineage_table
    if dest_ds and not fld.empty:
        view = fld[fld["Target Dataset"].astype(str) == dest_ds]
        if view.empty:
            view = fld[fld["Target Dataset"].astype(str).str.contains(dest_ds.split(".")[-1], case=False, na=False)]
        if not view.empty:
            show_table(view, "Field lineage into this asset")

    serving_tabs(str(asset.get("Result Location", "")), asset_id)
