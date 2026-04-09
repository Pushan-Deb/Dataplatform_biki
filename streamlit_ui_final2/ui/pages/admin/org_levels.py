import streamlit as st
import pandas as pd
from ui.components.editors import list_editor_table
from ui.components.tables import show_table


def page_org_levels():
    st.title("Admin – Org Levels (Hierarchical)")
    hierarchy = st.session_state.org_hierarchy
    l1 = st.selectbox("Level 1", list(hierarchy.keys()), key="org_l1_pick")
    l2s = list(hierarchy.get(l1, {}).keys())
    l2s = list_editor_table(f"Level 2 under {l1}", l2s, key=f"org_{l1}_l2_editor")

    new_map = {}
    for l2 in l2s:
        new_map[l2] = hierarchy.get(l1, {}).get(l2, [])
    hierarchy[l1] = new_map

    if l2s:
        l2_pick = st.selectbox("Choose Level 2 to edit Level 3", l2s, key=f"org_{l1}_l2_pick")
        l3s = hierarchy[l1].get(l2_pick, [])
        l3s = list_editor_table(f"Level 3 under {l1} → {l2_pick}", l3s, key=f"org_{l1}_{l2_pick}_l3_editor")
        hierarchy[l1][l2_pick] = l3s

    st.session_state.org_hierarchy = hierarchy

    rows = []
    for a, l2map in hierarchy.items():
        for b, l3list in l2map.items():
            if l3list:
                for c in l3list:
                    rows.append({"Level 1": a, "Level 2": b, "Level 3": c})
            else:
                rows.append({"Level 1": a, "Level 2": b, "Level 3": ""})
    show_table(pd.DataFrame(rows), "Current hierarchy")
