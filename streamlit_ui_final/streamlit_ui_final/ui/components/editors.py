import streamlit as st
import pandas as pd


def list_editor_table(title: str, items: list[str], key: str):
    st.markdown(f"### {title}")
    df = pd.DataFrame({"Value": list(items)})
    edited = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=key)
    values = []
    for v in edited["Value"].tolist():
        s = str(v).strip()
        if s:
            values.append(s)
    out = []
    for v in values:
        if v not in out:
            out.append(v)
    return out


def kv_editor_table(title: str, data: dict, key: str):
    st.markdown(f"### {title}")
    df = pd.DataFrame([{"Key": k, "Value": v} for k, v in data.items()])
    edited = st.data_editor(df, num_rows="dynamic", use_container_width=True, key=key)
    out = {}
    for _, row in edited.iterrows():
        k = str(row.get("Key", "")).strip()
        if k:
            out[k] = row.get("Value", "")
    return out
