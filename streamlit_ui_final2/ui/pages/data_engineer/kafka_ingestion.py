import streamlit as st
import pandas as pd
import requests
from ui.services.jobs import add_job, ensure_dummy_jobs
from ui.services.lineage import add_lineage_edge
from ui.services.utils import with_run_ts
from ui.state import enforce
from ui.config import KAFKA_BOOTSTRAP, SPARK_CUSTOM_API


def success_box(markdown_text: str):
    st.markdown(f"<div class='successbox'>{markdown_text}</div>", unsafe_allow_html=True)


def page_kafka_ingestion():
    st.title("Kafka Streaming Ingestion")
    st.caption(f"Kafka bootstrap: {KAFKA_BOOTSTRAP} | Spark Custom API: {SPARK_CUSTOM_API}")
    ensure_dummy_jobs()
    enforce("submit_ingestion")

    hierarchy = st.session_state.org_hierarchy

    topic = st.text_input("Kafka topic", value="crm.contacts", key="kafka_topic")
    create_topic = st.checkbox("Create topic (mock)", value=True, key="kafka_create_topic")
    filters = st.text_input("Optional filters", value="event_type = 'UPSERT'", key="kafka_filters")

    l1 = st.selectbox("Level 1", list(hierarchy.keys()), key="kafka_l1")
    l2 = st.selectbox("Level 2", list(hierarchy.get(l1, {}).keys()), key="kafka_l2")
    level3s = hierarchy.get(l1, {}).get(l2, [])
    l3 = st.selectbox("Level 3", level3s if level3s else [""], key="kafka_l3")
    dest_dataset = st.text_input("Destination dataset name", value="contacts_stream", key="kafka_dest_dataset")

    base_prefix = f"s3://minio/{l1.lower()}/{l2}"
    if str(l3).strip():
        base_prefix += f"/{l3}"
    dest_base = f"{base_prefix}/{dest_dataset}/"

    st.dataframe(pd.DataFrame([{
        "Topic": topic,
        "Create topic": "Yes" if create_topic else "No",
        "Filters": filters,
        "Output (base)": dest_base,
        "Suggested unique path": with_run_ts(dest_base)
    }]), use_container_width=True, hide_index=True)

    if st.button("Start streaming job", type="primary", key="kafka_submit"):
        API_BASE = SPARK_CUSTOM_API

        try:
            topic_resp = requests.post(
                f"{API_BASE}/api/topics/create-if-not-exists",
                json={
                    "topic": topic,
                    "partitions": 3,
                    "replicationFactor": 1
                },
                timeout=10
            )

            if topic_resp.status_code != 200:
                st.error("❌ Topic creation failed")
                st.stop()

            stream_resp = requests.post(
                f"{API_BASE}/api/stream/start",
                json={
                    "topic": topic,
                    "partitions": 3,
                    "replicationFactor": 1
                },
                timeout=10
            )

            if stream_resp.status_code != 200:
                st.error("❌ Streaming failed")
                st.stop()

            topic_result = topic_resp.json()
            topic_status = topic_result.get("status", "")

            if topic_status == "EXISTS":
                st.warning("⚠️ Topic already exists")
            elif topic_status == "CREATED":
                st.success("✅ Topic created successfully")

            result = stream_resp.json()

            job_id, link, result_loc = add_job(
                "Kafka Stream",
                "Kafka Connect / Flink",
                dest_base,
                status="RUNNING",
                visibility="Team",
                source=f"KafkaTopic:{topic}",
                destination=dest_base
            )

            add_lineage_edge("Kafka Topic", topic, "Job", job_id, "feeds")
            add_lineage_edge("Job", job_id, "Dataset", dest_base, "writes_to")

            success_box(
                f"✅ Streaming started successfully.<br>"
                f"Status: <b>{result.get('status')}</b><br>"
                f"<a href='{link}'>Open Job Details</a>"
            )

        except Exception as e:
            st.error(f"Connection error: {str(e)}")
