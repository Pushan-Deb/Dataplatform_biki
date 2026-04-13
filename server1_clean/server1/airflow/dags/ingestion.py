"""
Ingestion scheduler DAG: runs every minute and triggers Airbyte syncs for due connections.
All connections live in connection_summary_index; schedules are evaluated here.
"""
from __future__ import annotations

from datetime import datetime, timezone
import time
import logging
import requests
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from platform_env import env_int, env_str, require_env, load_platform_env
from platform_http import build_session

try:
    from croniter import croniter  # optional
except Exception:
    croniter = None

load_platform_env()

def _settings() -> dict:
    return {
        "airbyte_base_url": env_str("AIRBYTE_BASE_URL_INTERNAL") or require_env("AIRBYTE_BASE_URL"),
        "airbyte_workspace_id": require_env("AIRBYTE_WORKSPACE_ID"),
        "delta_bucket": require_env("DELTA_BUCKET"),
        "delta_base_prefix": env_str("DELTA_BASE_PREFIX", ""),
        "hive_db": require_env("HIVE_DB"),
        "airflow_base_url": env_str("AIRFLOW_BASE_URL_INTERNAL") or require_env("AIRFLOW_BASE_URL"),
        "airflow_username": require_env("AIRFLOW_USERNAME"),
        "airflow_password": require_env("AIRFLOW_PASSWORD"),
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
    last_err = None
    for attempt in range(3):
        try:
            resp = _airbyte_session().post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 + attempt)
    raise last_err


def _airbyte_get(path: str, timeout: int = 30):
    url = f"{_settings()['airbyte_base_url']}{path}"
    last_err = None
    for attempt in range(3):
        try:
            resp = _airbyte_session().get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        except requests.RequestException as e:
            last_err = e
            time.sleep(2 + attempt)
    raise last_err


def _fetch_airbyte_connection_details(connection_id: str) -> dict:
    return _airbyte_post("/api/v1/connections/get", {"connectionId": connection_id}, timeout=30) or {}


def _fetch_airbyte_destination(destination_id: str) -> dict:
    return _airbyte_post("/api/v1/destinations/get", {"destinationId": destination_id}, timeout=30) or {}


def _discover_catalog(source_id: str) -> dict:
    return _airbyte_post("/api/v1/sources/discover_schema", {"sourceId": source_id}, timeout=120) or {}

def _list_jobs_for_connection(connection_id: str, page_size: int = 5) -> list[dict]:
    payload = {
        "configTypes": ["sync"],
        "configId": connection_id,
        "pagination": {"pageSize": page_size},
    }
    resp = _airbyte_post("/api/v1/jobs/list", payload, timeout=30) or {}
    return resp.get("jobs", []) or []


def _create_connection(conn_name: str, source_id: str, dest_id: str, stream_name: str, namespace: str | None):
    settings = _settings()
    catalog = _discover_catalog(source_id)
    streams = (catalog.get("catalog") or {}).get("streams", []) or []
    chosen_stream = None
    for s in streams:
        stream = s.get("stream", {}) or {}
        name = stream.get("name")
        ns = stream.get("namespace") or ""
        if name == stream_name and (not namespace or ns == namespace):
            chosen_stream = stream
            break
    if not chosen_stream:
        raise ValueError(f"Stream not found in catalog: {namespace}.{stream_name}")

    cfg_stream = {
        "stream": chosen_stream,
        "config": {
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "cursorField": [],
            "primaryKey": chosen_stream.get("sourceDefinedPrimaryKey") or chosen_stream.get("primaryKey") or [],
            "selected": True,
        },
    }
    payload = {
        "name": conn_name,
        "sourceId": source_id,
        "destinationId": dest_id,
        "syncCatalog": {"streams": [cfg_stream]},
        "namespaceDefinition": "source",
        "namespaceFormat": "",
        "prefix": "",
        "scheduleType": "manual",
        "status": "active",
    }
    res = _airbyte_post("/api/v1/connections/create", payload, timeout=60)
    return res.get("connectionId")


def _derive_paths_and_streams(connection: dict) -> tuple[str, list[str]]:
    settings = _settings()
    dest_id = connection.get("destinationId")
    dest = _fetch_airbyte_destination(dest_id) if dest_id else {}
    cfg = dest.get("destination", {}).get("connectionConfiguration", {}) or dest.get("connectionConfiguration", {}) or {}
    bucket = cfg.get("s3_bucket_name") or cfg.get("bucket") or "airbyte"
    prefix = cfg.get("s3_bucket_path") or cfg.get("path") or ""
    prefix = prefix.strip("/")
    if prefix:
        base = f"s3a://{bucket}/{prefix}"
        delta_base = f"s3a://{settings['delta_bucket']}/{prefix}"
    else:
        base = f"s3a://{bucket}"
        delta_base = f"s3a://{settings['delta_bucket']}"
    if settings["delta_base_prefix"]:
        delta_base = f"{delta_base.rstrip('/')}/{settings['delta_base_prefix'].strip('/')}"

    streams = []
    sync_catalog = (connection.get("syncCatalog") or {}).get("streams", []) or []
    for stream in sync_catalog:
        s = stream.get("stream", {}) or {}
        name = s.get("name")
        if not name:
            continue
        ns = s.get("namespace")
        if ns:
            streams.append(f"{ns}/{name}")
        else:
            streams.append(name)
    return (base, delta_base), streams


def _upsert_job_summary(cur, row: dict):
    sql = """
        SELECT sp_upsert_job_summary_index(
            %(tenant_id)s, %(job_id)s, %(job_type)s, %(orchestrator)s, %(status)s,
            %(submitted_by_user_id)s, %(submitted_by_role)s, %(submitted_by_team)s, %(visibility)s,
            %(source)s, %(destination)s, %(result_location)s, %(open_job_link)s,
            %(airflow_dag_id)s, %(airflow_dag_run_id)s, %(airbyte_connection_id)s, %(airbyte_job_id)s,
            %(created_at)s, %(last_seen_at)s
        );
    """
    cur.execute(sql, row)


def _upsert_connection_summary(cur, row: dict):
    sql = """
        SELECT sp_upsert_connection_summary_index(
            %(tenant_id)s, %(connection_id)s, %(airbyte_connection_id)s, %(connection_name)s,
            %(source_name)s, %(destination_name)s, %(schedule_type)s, %(schedule_value)s,
            %(last_sync_status)s, %(last_sync_started_at)s, %(last_sync_ended_at)s,
            %(created_by_user_id)s, %(updated_by_user_id)s, %(created_at)s, %(last_seen_at)s
        );
    """
    cur.execute(sql, row)


def _is_due(now: datetime, schedule_type: str, schedule_value: str, last_started: datetime | None) -> bool:
    if schedule_type == "interval":
        try:
            minutes = int(schedule_value)
            if minutes <= 0:
                return False
            if not last_started:
                return True
            return (now - last_started).total_seconds() >= minutes * 60
        except Exception:
            return False
    if schedule_type == "cron":
        if not croniter or not schedule_value:
            return False
        base = last_started or now
        itr = croniter(schedule_value, base)
        next_time = itr.get_next(datetime)
        return next_time <= now
    return False


def _trigger_downstream(conf: dict) -> None:
    settings = _settings()
    url = f"{settings['airflow_base_url']}/api/v1/dags/post_ingestion/dagRuns"
    resp = requests.post(
        url,
        auth=(settings["airflow_username"], settings["airflow_password"]),
        json={"conf": conf},
        timeout=15,
    )
    resp.raise_for_status()


def schedule_and_trigger(**context):
    settings = _settings()
    log = logging.getLogger("airflow.task")
    now = datetime.now(timezone.utc)
    dag_run_id = (context.get("dag_run") or {}).run_id
    conf = (context.get("dag_run") or {}).conf or {}

    with _db_conn() as conn:
        with conn.cursor() as cur:
            # Provision mode: UI triggers ingestion DAG with a conf payload to create a connection
            if conf.get("source") and conf.get("destination") and conf.get("stream"):
                source = conf.get("source") or {}
                destination = conf.get("destination") or {}
                stream = conf.get("stream") or {}
                schedule = conf.get("schedule") or {}

                tenant_id = conf.get("tenantId") or "default"
                user_id = conf.get("userId") or ""
                ui_job_id = conf.get("ui_job_id") or f"JOB-{int(now.timestamp())}"
                visibility = conf.get("visibility") or "Private"

                source_id = source.get("id") or source.get("sourceId")
                dest_id = destination.get("id") or destination.get("destinationId")
                if not source_id or not dest_id:
                    raise ValueError("source.id and destination.id are required for provisioning")

                stream_name = stream.get("name")
                namespace = stream.get("namespace")
                src_name = source.get("name") or "source"
                dst_name = destination.get("name") or "destination"
                conn_name = f"{src_name} -> {dst_name} ({stream_name})"

                conn_id = _create_connection(conn_name, source_id, dest_id, stream_name, namespace)

                schedule_type = schedule.get("type") or "interval"
                schedule_value = str(schedule.get("interval_minutes") or schedule.get("cron") or "")
                platform_conn_id = conf.get("connection_id") or f"CONN-{conn_id[:8]}"

                _upsert_connection_summary(cur, {
                    "tenant_id": tenant_id,
                    "connection_id": platform_conn_id,
                    "airbyte_connection_id": conn_id,
                    "connection_name": conn_name,
                    "source_name": src_name,
                    "destination_name": dst_name,
                    "schedule_type": schedule_type,
                    "schedule_value": schedule_value,
                    "last_sync_status": None,
                    "last_sync_started_at": None,
                    "last_sync_ended_at": None,
                    "created_by_user_id": user_id,
                    "updated_by_user_id": user_id,
                    "created_at": now,
                    "last_seen_at": now,
                })

                _upsert_job_summary(cur, {
                    "tenant_id": tenant_id,
                    "job_id": ui_job_id,
                    "job_type": conf.get("job_type") or "Ingestion",
                    "orchestrator": "Airflow + Airbyte",
                    "status": "PROVISIONED",
                    "submitted_by_user_id": user_id,
                    "submitted_by_role": conf.get("role"),
                    "submitted_by_team": tenant_id,
                    "visibility": visibility,
                    "source": stream.get("raw"),
                    "destination": dst_name,
                    "result_location": conf.get("result_location"),
                    "open_job_link": conf.get("open_job_link"),
                    "airflow_dag_id": "ingestion",
                    "airflow_dag_run_id": dag_run_id,
                    "airbyte_connection_id": conn_id,
                    "airbyte_job_id": None,
                    "created_at": now,
                    "last_seen_at": now,
                })

                conn.commit()
                log.info("Provisioned connection %s via ingestion DAG", conn_id)
                return {"status": "provisioned", "airbyte_connection_id": conn_id}

            cur.execute(
                """
                SELECT tenant_id, connection_id, airbyte_connection_id, connection_name,
                       source_name, destination_name, schedule_type, schedule_value,
                       last_sync_started_at, last_sync_status, created_by_user_id
                FROM connection_summary_index
                WHERE airbyte_connection_id IS NOT NULL
                """
            )
            rows = cur.fetchall()

            triggered = 0
            for row in rows:
                tenant_id, connection_id, ab_conn_id, conn_name, src_name, dst_name, sch_type, sch_val, last_started, last_status, created_by = row
                if not sch_type or not sch_val:
                    continue
                if not _is_due(now, sch_type, sch_val, last_started):
                    continue

                # Validate Airbyte connection status before trying to sync
                ab_conn = None
                ab_status = None
                try:
                    ab_conn = _fetch_airbyte_connection_details(ab_conn_id)
                    ab_status = (ab_conn.get("status") or "").lower()
                except Exception as exc:
                    log.warning("Unable to read Airbyte connection %s: %s", ab_conn_id, exc)

                if not ab_conn:
                    _upsert_connection_summary(cur, {
                        "tenant_id": tenant_id,
                        "connection_id": connection_id,
                        "airbyte_connection_id": ab_conn_id,
                        "connection_name": conn_name,
                        "source_name": src_name,
                        "destination_name": dst_name,
                        "schedule_type": sch_type,
                        "schedule_value": sch_val,
                        "last_sync_status": "MISSING",
                        "last_sync_started_at": now,
                        "last_sync_ended_at": None,
                        "created_by_user_id": created_by,
                        "updated_by_user_id": created_by,
                        "created_at": now,
                        "last_seen_at": now,
                    })
                    continue

                if ab_status and ab_status != "active":
                    log.info("Skipping sync for %s (status=%s)", ab_conn_id, ab_status)
                    _upsert_connection_summary(cur, {
                        "tenant_id": tenant_id,
                        "connection_id": connection_id,
                        "airbyte_connection_id": ab_conn_id,
                        "connection_name": conn_name,
                        "source_name": src_name,
                        "destination_name": dst_name,
                        "schedule_type": sch_type,
                        "schedule_value": sch_val,
                        "last_sync_status": f"INACTIVE:{ab_status}",
                        "last_sync_started_at": now,
                        "last_sync_ended_at": None,
                        "created_by_user_id": created_by,
                        "updated_by_user_id": created_by,
                        "created_at": now,
                        "last_seen_at": now,
                    })
                    continue

                job_id = f"JOB-{ab_conn_id[:8]}-{int(now.timestamp())}"
                try:
                    sync_resp = _airbyte_post("/api/v1/connections/sync", {"connectionId": ab_conn_id}, timeout=30)
                    job_info = (sync_resp or {}).get("job") or {}
                    ab_job_id = job_info.get("id") or sync_resp.get("jobId") or sync_resp.get("id")
                    status = "QUEUED"
                except requests.HTTPError as exc:
                    ab_job_id = None
                    status = "FAILED"
                    if exc.response is not None and exc.response.status_code == 409:
                        # Connection already syncing; grab latest running job id if available
                        jobs = _list_jobs_for_connection(ab_conn_id, page_size=5)
                        running = next((j for j in jobs if (j.get("job") or {}).get("status") == "running"), None)
                        if running:
                            ab_job_id = (running.get("job") or {}).get("id")
                            status = "RUNNING"
                        else:
                            status = "RUNNING"
                    log.warning("Airbyte sync failed for %s: %s", ab_conn_id, exc)
                except Exception as exc:
                    ab_job_id = None
                    status = "FAILED"
                    log.warning("Airbyte sync failed for %s: %s", ab_conn_id, exc)

                if ab_job_id:
                    job_id = f"JOB-{ab_job_id}"

                _upsert_job_summary(cur, {
                    "tenant_id": tenant_id,
                    "job_id": job_id,
                    "job_type": "Ingestion",
                    "orchestrator": "Airflow + Airbyte",
                    "status": status,
                    "submitted_by_user_id": created_by,
                    "submitted_by_role": None,
                    "submitted_by_team": tenant_id,
                    "visibility": "Private",
                    "source": src_name,
                    "destination": dst_name,
                    "result_location": None,
                    "open_job_link": None,
                    "airflow_dag_id": "ingestion",
                    "airflow_dag_run_id": dag_run_id,
                    "airbyte_connection_id": ab_conn_id,
                    "airbyte_job_id": str(ab_job_id) if ab_job_id else None,
                    "created_at": now,
                    "last_seen_at": now,
                })

                _upsert_connection_summary(cur, {
                    "tenant_id": tenant_id,
                    "connection_id": connection_id,
                    "airbyte_connection_id": ab_conn_id,
                    "connection_name": conn_name,
                    "source_name": src_name,
                    "destination_name": dst_name,
                    "schedule_type": sch_type,
                    "schedule_value": sch_val,
                    "last_sync_status": status,
                    "last_sync_started_at": now,
                    "last_sync_ended_at": None,
                    "created_by_user_id": created_by,
                    "updated_by_user_id": created_by,
                    "created_at": now,
                    "last_seen_at": now,
                })

                # Trigger downstream DAG and let it wait for Airbyte completion
                if ab_job_id:
                    try:
                        connection = _fetch_airbyte_connection_details(ab_conn_id)
                        (airbyte_base, delta_base), streams = _derive_paths_and_streams(connection)
                        if streams:
                            _trigger_downstream({
                                "tenant_id": tenant_id,
                                "connection_id": connection_id,
                                "airbyte_connection_id": ab_conn_id,
                                "airbyte_job_id": str(ab_job_id),
                                "source_name": src_name,
                                "destination_name": dst_name,
                                "airbyte_base": airbyte_base,
                                "delta_base": delta_base,
                                "streams": streams,
                                "hive_db": settings["hive_db"],
                            })
                    except Exception as exc:
                        log.warning("Failed to trigger post_ingestion for %s: %s", ab_conn_id, exc)

                triggered += 1

            conn.commit()
            log.info("Triggered %s scheduled connections", triggered)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "ingestion",
    default_args=default_args,
    description="Central ingestion scheduler (triggers due Airbyte connections)",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["airbyte", "scheduler"],
)

PythonOperator(
    task_id="trigger_due_connections",
    python_callable=schedule_and_trigger,
    dag=dag,
)
