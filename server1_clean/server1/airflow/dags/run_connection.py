"""
Run Airbyte sync for an existing connection (triggered by UI via Airflow API).
"""
from __future__ import annotations

from datetime import datetime, timezone
import time
import requests
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from platform_env import env_int, env_str, require_env, load_platform_env
from platform_http import build_session

load_platform_env()

def _settings() -> dict:
    return {
        "airbyte_base_url": env_str("AIRBYTE_BASE_URL_INTERNAL") or require_env("AIRBYTE_BASE_URL"),
        "db_host": env_str("POSTGRES_HOST_INTERNAL") or require_env("POSTGRES_HOST"),
        "db_port": env_int("POSTGRES_PORT_INTERNAL") if env_str("POSTGRES_HOST_INTERNAL") else env_int("POSTGRES_PORT"),
        "db_user": require_env("POSTGRES_USER"),
        "db_password": require_env("POSTGRES_PASSWORD"),
        "db_name": require_env("CONNECTION_INDEX_DB"),
    }


def _airbyte_session():
    return build_session("AIRBYTE")


def _db_conn():
    settings = _settings()
    return psycopg2.connect(
        host=settings["db_host"],
        port=settings["db_port"],
        user=settings["db_user"],
        password=settings["db_password"],
        dbname=settings["db_name"],
    )


def _airbyte_post(path: str, payload: dict, timeout: int = 30):
    url = f"{_settings()['airbyte_base_url']}{path}"
    resp = _airbyte_session().post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _upsert_job_summary(row: dict):
    sql = """
        SELECT sp_upsert_job_summary_index(
            %(tenant_id)s, %(job_id)s, %(job_type)s, %(orchestrator)s, %(status)s,
            %(submitted_by_user_id)s, %(submitted_by_role)s, %(submitted_by_team)s, %(visibility)s,
            %(source)s, %(destination)s, %(result_location)s, %(open_job_link)s,
            %(airflow_dag_id)s, %(airflow_dag_run_id)s, %(airbyte_connection_id)s, %(airbyte_job_id)s,
            %(created_at)s, %(last_seen_at)s
        );
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, row)
            conn.commit()


def _poll_job(job_id: int, timeout_sec: int = 1800, poll_sec: int = 15) -> str:
    deadline = time.time() + timeout_sec
    status = "running"
    while time.time() < deadline:
        job = _airbyte_post("/api/v1/jobs/get_light", {"id": int(job_id)}, timeout=30)
        status = (job.get("job") or {}).get("status") or "running"
        if status in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(poll_sec)
    return status


def run_connection(**context):
    conf = (context.get("dag_run") or {}).conf or {}
    conn_id = conf.get("airbyte_connection_id")
    ui_job_id = conf.get("ui_job_id")
    if not conn_id:
        raise ValueError("airbyte_connection_id is required")

    sync_resp = _airbyte_post("/api/v1/connections/sync", {"connectionId": conn_id}, timeout=30)
    job_info = (sync_resp or {}).get("job") or {}
    job_id = job_info.get("id") or sync_resp.get("jobId") or sync_resp.get("id")
    status = _poll_job(int(job_id)) if job_id else "running"

    _upsert_job_summary({
        "tenant_id": conf.get("tenantId") or "default",
        "job_id": ui_job_id,
        "job_type": conf.get("job_type") or "Ingestion",
        "orchestrator": "Airflow + Airbyte",
        "status": status.upper() if isinstance(status, str) else "RUNNING",
        "submitted_by_user_id": conf.get("userId"),
        "submitted_by_role": conf.get("role"),
        "submitted_by_team": conf.get("tenantId"),
        "visibility": conf.get("visibility") or "Private",
        "source": conf.get("source"),
        "destination": conf.get("destination"),
        "result_location": conf.get("result_location"),
        "open_job_link": conf.get("open_job_link"),
        "airflow_dag_id": "run_connection",
        "airflow_dag_run_id": (context.get("dag_run") or {}).run_id,
        "airbyte_connection_id": conn_id,
        "airbyte_job_id": str(job_id) if job_id else None,
        "created_at": datetime.now(timezone.utc),
        "last_seen_at": datetime.now(timezone.utc),
    })

    return {"status": status, "airbyte_job_id": job_id}


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "run_connection",
    default_args=default_args,
    description="Run Airbyte sync via Airflow",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["airbyte", "run"],
)

PythonOperator(
    task_id="run_sync",
    python_callable=run_connection,
    dag=dag,
)
