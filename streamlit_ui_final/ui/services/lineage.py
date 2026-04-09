import streamlit as st
import pandas as pd
import uuid
from datetime import datetime


def add_lineage_edge(from_type: str, from_id: str, to_type: str, to_id: str, relation: str):
    edge_id = f"LIN-{str(uuid.uuid4())[:8]}"
    created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "Edge Id": edge_id,
        "From Type": from_type,
        "From Id": from_id,
        "To Type": to_type,
        "To Id": to_id,
        "Relation": relation,
        "Created At": created,
    }
    st.session_state.lineage_table = pd.concat([st.session_state.lineage_table, pd.DataFrame([row])],
                                                ignore_index=True)


def add_field_lineage(src_dataset: str, src_col: str, tgt_dataset: str, tgt_col: str, transform: str):
    edge_id = f"FLN-{str(uuid.uuid4())[:8]}"
    created = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "Edge Id": edge_id,
        "Source Dataset": src_dataset,
        "Source Column": src_col,
        "Target Dataset": tgt_dataset,
        "Target Column": tgt_col,
        "Transform": transform,
        "Created At": created,
    }
    st.session_state.field_lineage_table = pd.concat([
        st.session_state.field_lineage_table, pd.DataFrame([row])
    ], ignore_index=True)
    return edge_id


def add_field_lineage_edge(src_dataset: str, src_col: str, tgt_dataset: str, tgt_col: str, transform: str,
                            created: str | None = None):
    if created is None:
        return add_field_lineage(src_dataset, src_col, tgt_dataset, tgt_col, transform)
    edge_id = f"FLN-{str(uuid.uuid4())[:8]}"
    row = {
        "Edge Id": edge_id,
        "Source Dataset": src_dataset,
        "Source Column": src_col,
        "Target Dataset": tgt_dataset,
        "Target Column": tgt_col,
        "Transform": transform,
        "Created At": created,
    }
    st.session_state.field_lineage_table = pd.concat([
        st.session_state.field_lineage_table, pd.DataFrame([row])
    ], ignore_index=True)
    return edge_id


def lineage_for_node(node_id: str) -> pd.DataFrame:
    df = st.session_state.lineage_table
    if df.empty:
        return df
    mask = (df["From Id"] == node_id) | (df["To Id"] == node_id)
    return df[mask].copy()


def ensure_dummy_lineage():
    if not st.session_state.lineage_table.empty:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    edges = [
        ("Job", "JOB-DEMO-0001", "Dataset", "bronze.sales.orders", "writes_to"),
        ("Dataset", "bronze.sales.orders", "Dataset", "silver.sales.orders_clean", "transforms_to"),
        ("Dataset", "silver.sales.orders_clean", "Dataset", "gold.sales.customer_agg", "aggregates_to"),
        ("Dataset", "gold.sales.customer_agg", "Feature", "customer_lifetime_value", "feeds_feature"),
        ("Feature", "customer_lifetime_value", "Model", "churn_model", "feeds_model"),
        ("Job", "JOB-DEMO-0002", "Asset", "AST-DEMO-0001", "publishes"),
    ]
    for ft, fid, tt, tid, rel in edges:
        edge_id = f"LIN-{str(uuid.uuid4())[:8]}"
        row = {"Edge Id": edge_id, "From Type": ft, "From Id": fid, "To Type": tt, "To Id": tid, "Relation": rel,
               "Created At": now}
        st.session_state.lineage_table = pd.concat([st.session_state.lineage_table, pd.DataFrame([row])],
                                                    ignore_index=True)


def ensure_dummy_field_lineage():
    if not st.session_state.field_lineage_table.empty:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    samples = [
        ("sales.orders", "order_id", "gold.sales.customer_agg", "order_id", "copy"),
        ("sales.orders", "customer_id", "gold.sales.customer_agg", "customer_id", "copy"),
        ("sales.orders", "amount", "gold.sales.customer_agg", "total_revenue", "SUM(amount) GROUP BY customer_id"),
        ("gold.sales.customer_agg", "total_revenue", "gold.sales.features", "clv", "rename(total_revenue→clv)"),
        ("silver.crm.customers", "customer_id", "gold.sales.features", "customer_id", "copy"),
    ]
    for a, b, c, d, e in samples:
        add_field_lineage_edge(a, b, c, d, e, created=now)
