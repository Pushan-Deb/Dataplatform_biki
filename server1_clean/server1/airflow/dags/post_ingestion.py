"""
Post-ingestion DAG: Delta conversion -> Hive registration -> OpenMetadata lineage.
Triggered by ingestion DAG after Airbyte sync succeeds.
"""
from __future__ import annotations

from datetime import datetime
import logging
import time
import requests
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from platform_env import env_int, env_str, require_env, load_platform_env
from platform_http import build_session

load_platform_env()
TRINO_SESSION = build_session("TRINO")
AIRBYTE_SESSION = build_session("AIRBYTE")
SPARK_MINIO_ENDPOINT = env_str("MINIO_ENDPOINT_EXTERNAL") or env_str("MINIO_ENDPOINT")
SPARK_HIVE_METASTORE_URI = env_str("HIVE_METASTORE_URI_EXTERNAL") or env_str("HIVE_METASTORE_URI")


def _split_top_level(value: str, delimiter: str = ",") -> list[str]:
    parts = []
    current = []
    depth = 0
    for ch in value:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == delimiter and depth == 0:
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
            continue
        current.append(ch)
    token = "".join(current).strip()
    if token:
        parts.append(token)
    return parts


def _trino_to_om_type(trino_type: str) -> str:
    t = (trino_type or "").strip().lower()
    if t.startswith("row("):
        return "STRUCT"
    if t.startswith("array("):
        return "ARRAY"
    if t.startswith("map("):
        return "MAP"
    if t.startswith("varchar") or t.startswith("char") or t == "json":
        return "STRING"
    if t.startswith("bigint"):
        return "BIGINT"
    if t.startswith("integer") or t.startswith("int"):
        return "INT"
    if t.startswith("smallint"):
        return "SMALLINT"
    if t.startswith("tinyint"):
        return "TINYINT"
    if t.startswith("double"):
        return "DOUBLE"
    if t.startswith("real"):
        return "FLOAT"
    if t.startswith("decimal"):
        return "DECIMAL"
    if t.startswith("boolean"):
        return "BOOLEAN"
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    if t.startswith("date"):
        return "DATE"
    if t.startswith("time"):
        return "TIME"
    if t.startswith("varbinary"):
        return "BINARY"
    return "STRING"


def _parse_trino_type(trino_type: str) -> dict:
    t = (trino_type or "").strip()
    column = {
        "dataType": _trino_to_om_type(t),
        "dataTypeDisplay": t,
        "children": [],
    }
    lower = t.lower()
    if lower.startswith("row(") and t.endswith(")"):
        inner = t[t.find("(") + 1:-1].strip()
        children = []
        for field in _split_top_level(inner):
            if " " not in field:
                continue
            name, child_type = field.split(" ", 1)
            child = _parse_trino_type(child_type)
            child["name"] = name.strip('"')
            children.append(child)
        column["children"] = children
    return column


def _query_trino_rows(statement: str, catalog: str, schema: str) -> list[list]:
    trino_base = env_str("TRINO_BASE_URL_INTERNAL") or require_env("TRINO_BASE_URL")
    headers = {
        "X-Trino-User": require_env("TRINO_USER"),
        "X-Trino-Catalog": catalog,
        "X-Trino-Schema": schema,
    }
    response = TRINO_SESSION.post(
        f"{trino_base}/v1/statement",
        data=statement,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows = list(payload.get("data") or [])
    next_uri = payload.get("nextUri")
    while next_uri:
        page = TRINO_SESSION.get(next_uri, timeout=30)
        page.raise_for_status()
        payload = page.json()
        rows.extend(payload.get("data") or [])
        next_uri = payload.get("nextUri")
    return rows


def _execute_trino_statement(statement: str, catalog: str, schema: str) -> dict:
    trino_base = env_str("TRINO_BASE_URL_INTERNAL") or require_env("TRINO_BASE_URL")
    headers = {
        "X-Trino-User": require_env("TRINO_USER"),
        "X-Trino-Catalog": catalog,
        "X-Trino-Schema": schema,
    }
    response = TRINO_SESSION.post(
        f"{trino_base}/v1/statement",
        data=statement,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    next_uri = payload.get("nextUri")
    while next_uri:
        page = TRINO_SESSION.get(next_uri, timeout=30)
        page.raise_for_status()
        payload = page.json()
        next_uri = payload.get("nextUri")
    error = payload.get("error")
    if error:
        raise RuntimeError(error.get("message") or str(error))
    return payload


def _fetch_trino_columns(table_name: str, schema: str, catalog: str = "delta") -> list[dict]:
    rows = _query_trino_rows(f"DESCRIBE {catalog}.{schema}.{table_name}", catalog=catalog, schema=schema)
    columns = []
    for row in rows:
        if not row or len(row) < 2:
            continue
        col_name = row[0]
        col_type = row[1]
        parsed = _parse_trino_type(col_type)
        parsed["name"] = col_name.strip('"')
        columns.append(parsed)
    return columns


def run_post_ingestion(**context):
    log = logging.getLogger("airflow.task")
    conf = (context.get("dag_run") or {}).conf or {}

    airbyte_base = conf.get("airbyte_base")
    delta_base = conf.get("delta_base")
    streams = conf.get("streams") or []
    hive_db = conf.get("hive_db") or "default"
    source_name = conf.get("source_name")
    airbyte_job_id = conf.get("airbyte_job_id")
    airbyte_connection_id = conf.get("airbyte_connection_id")
    airbyte_base_url = env_str("AIRBYTE_BASE_URL_INTERNAL") or require_env("AIRBYTE_BASE_URL")

    if not airbyte_base or not delta_base or not streams:
        log.warning("Missing config for post_ingestion: %s", conf)
        return {"status": "skipped"}

    import sys
    sys.path.insert(0, "/opt/airflow")
    from integrations import SparkAPI, OpenMetadataAPI

    spark_api = SparkAPI()

    def wait_for_submission(submission_id: str, label: str, timeout_s: int = 1800):
        if not submission_id:
            log.warning("No Spark submission id for %s; skipping wait", label)
            return {"state": "unknown"}
        deadline = time.time() + timeout_s
        last_state = "unknown"
        last_payload = {}
        while time.time() < deadline:
            try:
                payload = spark_api.get_job_status(submission_id)
                last_payload = payload or {}
                last_state = (last_payload.get("submissionState")
                              or last_payload.get("driverState")
                              or last_state)
                if last_state in ("FINISHED", "FAILED", "KILLED", "ERROR"):
                    log.info("Spark %s finished with state=%s", label, last_state)
                    return {"state": last_state, "payload": last_payload}
            except Exception as exc:
                log.warning("Spark status check failed for %s: %s", label, exc)
            time.sleep(10)
        log.warning("Spark %s did not finish in %ss (last_state=%s)", label, timeout_s, last_state)
        return {"state": "timeout", "payload": last_payload}

    # Wait for Airbyte job to finish (if job id present)
    if airbyte_job_id:
        deadline = time.time() + 1800
        status = "running"
        while time.time() < deadline:
            try:
                resp = AIRBYTE_SESSION.post(
                    f"{airbyte_base_url}/api/v1/jobs/get_light",
                    json={"id": int(airbyte_job_id)},
                    timeout=30,
                )
                resp.raise_for_status()
                job = resp.json()
                status = (job.get("job") or {}).get("status") or "running"
            except Exception:
                status = "running"
            if status in ("succeeded", "failed", "cancelled"):
                break
            time.sleep(15)
        if status != "succeeded":
            log.warning("Airbyte job not succeeded (status=%s). Skipping post-ingestion.", status)
            return {"status": status}

    # Delta conversion
    delta_submit = spark_api.submit_job(
        app_resource="file:///opt/spark-jobs/airbyte_to_delta.py",
        app_name="airbyte_to_delta",
        env_vars={
            "AIRBYTE_MINIO_BASE": airbyte_base,
            "DELTA_OUTPUT_BASE": delta_base,
            "DELTA_WRITE_MODE": "append",
            "AIRBYTE_READ_MODE": env_str("AIRBYTE_READ_MODE", "boto3"),
            "DELTA_OUTPUT_MODE": env_str("DELTA_OUTPUT_MODE", "boto3"),
            "STREAMS": ",".join(streams),
            "MINIO_ENDPOINT": SPARK_MINIO_ENDPOINT,
            "MINIO_ACCESS_KEY": require_env("MINIO_ACCESS_KEY"),
            "MINIO_SECRET_KEY": require_env("MINIO_SECRET_KEY"),
        },
    )
    delta_state = wait_for_submission(delta_submit.get("submissionId"), "airbyte_to_delta")
    if delta_state.get("state") not in ("FINISHED", "unknown"):
        log.warning("Delta conversion did not finish cleanly: %s", delta_state.get("state"))
        raise RuntimeError(f"Delta conversion failed: {delta_state.get('state')}")

    # Register Delta tables in Hive Metastore via Spark so Trino can discover them.
    register_submit = spark_api.submit_job(
        app_resource="file:///opt/spark-jobs/register_delta_table.py",
        app_name="register_delta_tables",
        env_vars={
            "DELTA_OUTPUT_BASE": delta_base,
            "TABLES": ",".join(streams),
            "HIVE_DB": hive_db,
            "MINIO_ENDPOINT": SPARK_MINIO_ENDPOINT,
            "MINIO_ACCESS_KEY": require_env("MINIO_ACCESS_KEY"),
            "MINIO_SECRET_KEY": require_env("MINIO_SECRET_KEY"),
            "HIVE_METASTORE_URI": SPARK_HIVE_METASTORE_URI,
        },
    )
    register_state = wait_for_submission(register_submit.get("submissionId"), "register_delta_table")
    if register_state.get("state") not in ("FINISHED", "unknown"):
        log.warning("Delta registration did not finish cleanly: %s", register_state.get("state"))
        raise RuntimeError(f"Delta registration failed: {register_state.get('state')}")

    for stream in streams:
        table_name = stream.split("/", 1)[-1].split(".", 1)[-1]
        # Wait for Trino/Hive metadata propagation before schema fetches.
        for _ in range(12):
            try:
                _query_trino_rows(
                    f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{hive_db}' AND table_name = '{table_name}'",
                    catalog="delta",
                    schema=hive_db,
                )
                break
            except Exception:
                time.sleep(5)

    # OpenMetadata lineage + metadata
    columns_by_stream = {}
    trino_columns_by_stream = {}
    if airbyte_connection_id:
        try:
            resp = AIRBYTE_SESSION.post(
                f"{airbyte_base_url}/api/v1/connections/get",
                json={"connectionId": airbyte_connection_id},
                timeout=60,
            )
            resp.raise_for_status()
            connection = resp.json() or {}
            sync_catalog = (connection.get("syncCatalog") or {}).get("streams", []) or []
            log.info("Airbyte connection returned %s streams", len(sync_catalog))

            def _map_type(raw):
                if isinstance(raw, list):
                    raw = [r for r in raw if r != "null"]
                    raw = raw[0] if raw else "string"
                if raw == "string":
                    return "STRING"
                if raw == "integer":
                    return "BIGINT"
                if raw == "number":
                    return "DOUBLE"
                if raw == "boolean":
                    return "BOOLEAN"
                if raw == "array":
                    return "ARRAY"
                if raw == "object":
                    return "STRUCT"
                return "STRING"

            for entry in sync_catalog:
                s = (entry.get("stream") or {})
                name = s.get("name")
                if not name:
                    continue
                namespace = s.get("namespace")
                stream_key = f"{namespace}/{name}" if namespace else name
                schema = s.get("jsonSchema") or {}
                props = schema.get("properties") or {}
                columns = []
                for col_name, col_schema in props.items():
                    col_type = _map_type(col_schema.get("type"))
                    columns.append({"name": col_name, "dataType": col_type})
                if columns:
                    columns_by_stream[stream_key] = columns
                    log.info("Mapped %s columns for %s", len(columns), stream_key)
            if not columns_by_stream:
                log.warning("No columns mapped from connection syncCatalog, trying source discover_schema")
                source_id = connection.get("sourceId")
                if source_id:
                    disc = AIRBYTE_SESSION.post(
                        f"{airbyte_base_url}/api/v1/sources/discover_schema",
                        json={"sourceId": source_id},
                        timeout=120,
                    )
                    disc.raise_for_status()
                    streams = (disc.json().get("catalog") or {}).get("streams", []) or []
                    for entry in streams:
                        s = (entry.get("stream") or {})
                        name = s.get("name")
                        if not name:
                            continue
                        namespace = s.get("namespace")
                        stream_key = f"{namespace}/{name}" if namespace else name
                        schema = s.get("jsonSchema") or {}
                        props = schema.get("properties") or {}
                        columns = []
                        for col_name, col_schema in props.items():
                            col_type = _map_type(col_schema.get("type"))
                            columns.append({"name": col_name, "dataType": col_type})
                        if columns:
                            columns_by_stream[stream_key] = columns
                            log.info("Mapped %s columns for %s (discover_schema)", len(columns), stream_key)
        except Exception as exc:
            log.warning("Failed to fetch Airbyte schema for OpenMetadata: %s", exc)

    try:
        om = OpenMetadataAPI()
        for stream in streams:
            stream_name = stream
            if "/" in stream:
                _, stream_name = stream.split("/", 1)
            elif "." in stream:
                _, stream_name = stream.split(".", 1)
            # Create a raw table entity to anchor lineage
            raw_name = f"raw_{stream_name}".replace("/", "_").replace(".", "_")
            raw_cols = columns_by_stream.get(stream)
            try:
                trino_columns_by_stream[stream] = _fetch_trino_columns(stream_name, hive_db, "delta")
            except Exception as exc:
                log.warning("Failed to fetch Trino schema for %s: %s", stream_name, exc)
                trino_columns_by_stream[stream] = []
            om.create_table(
                table_name=raw_name,
                database=hive_db,
                columns=raw_cols,
            )
            om.create_table(
                table_name=stream_name,
                database=hive_db,
                columns=trino_columns_by_stream.get(stream) or columns_by_stream.get(stream),
            )
            if stream in trino_columns_by_stream and trino_columns_by_stream.get(stream):
                om.update_table_columns(stream_name, trino_columns_by_stream.get(stream))
            elif stream in columns_by_stream:
                om.update_table_columns(stream_name, columns_by_stream.get(stream))
            cols_for_stream = columns_by_stream.get(stream) or []
            col_lineage = []
            for col in cols_for_stream:
                name = col.get("name")
                if not name:
                    continue
                col_lineage.append({
                    "fromColumns": [name],
                    "toColumn": f"_airbyte_data.{name}",
                    "function": "copy",
                })
            om.create_table_lineage(
                source_table=raw_name,
                target_table=stream_name,
                description="Auto lineage: raw -> delta",
                columns_lineage=col_lineage if col_lineage else None,
                source="OpenLineage",
            )
    except Exception as exc:
        log.warning("OpenMetadata update failed: %s", exc)

    return {"status": "ok", "streams": streams}


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "post_ingestion",
    default_args=default_args,
    description="Delta + Hive + Lineage after Airbyte sync",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["delta", "hive", "lineage"],
)

PythonOperator(
    task_id="delta_hive_lineage",
    python_callable=run_post_ingestion,
    dag=dag,
)
