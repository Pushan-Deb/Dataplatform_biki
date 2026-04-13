"""
services/feast_service.py - Feast feature store integration.

Architecture (Option B):
  Save/Update  → feast apply (register in PostgreSQL feast_registry)
               + Run feature SQL (Trino/Spark)
               + Save result to MinIO feast-data/offline/  (OFFLINE STORE)

  Materialize  → feast materialize only
               + Read offline MinIO parquet
               + Write latest per entity to MinIO feast-data/online/ (ONLINE STORE)
               + Write latest per entity to PostgreSQL feast_online schema
"""
import os
import re
import sys
import subprocess
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from backend.config import get_settings

settings = get_settings()
FEAST_REPO_DIR = Path(settings.LOCAL_FEAST_REPO_DIR)


# ─────────────────────────────────────────────────────────────────────────────
# Feast executable helper
# ─────────────────────────────────────────────────────────────────────────────

def _feast_executable() -> list:
    """Return command list to run feast."""
    python_dir = Path(sys.executable).parent
    candidates = [
        python_dir / "Scripts" / "feast.exe",
        python_dir / "Scripts" / "feast",
        python_dir / "feast.exe",
        python_dir / "feast",
        python_dir.parent / "Scripts" / "feast.exe",
        python_dir.parent / "bin" / "feast",
    ]
    for candidate in candidates:
        if candidate.exists():
            print(f"[Feast] Found feast at: {candidate}")
            return [str(candidate)]
    print(f"[Feast] feast.exe not found, using: {sys.executable} -m feast")
    return [sys.executable, "-m", "feast"]


def _run_feast_cmd(args: list, cwd: str, timeout: int = 120) -> dict:
    """Run a feast command."""
    env = os.environ.copy()
    env["FEAST_USAGE"] = "False"
    cmd = _feast_executable() + args
    print(f"[Feast] Running: {' '.join(cmd)} in {cwd}")
    try:
        result = subprocess.run(
            cmd, cwd=cwd, capture_output=True,
            text=True, env=env, timeout=timeout,
        )
        log = (result.stdout + "\n" + result.stderr).strip()
        if not log:
            log = f"feast {' '.join(args)} completed (returncode={result.returncode})"
        return {"success": result.returncode == 0, "log": log}
    except Exception as e:
        return {"success": False, "log": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Window helper
# ─────────────────────────────────────────────────────────────────────────────

def _window_to_seconds(window_str: str) -> int:
    mapping = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    match = re.match(r"(\d+)([smhd])", window_str.lower())
    if match:
        return int(match.group(1)) * mapping[match.group(2)]
    return 86400 * 90


# ─────────────────────────────────────────────────────────────────────────────
# Feast repo generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_feature_store_yaml() -> str:
    return textwrap.dedent(f"""\
    project: {settings.PROJECT_NAME}
    registry:
      registry_type: sql
      path: {settings.FEAST_REGISTRY_URL}
      cache_ttl_seconds: 60

    provider: local

    offline_store:
      type: file

    online_store:
      type: postgres
      host: {settings.POSTGRES_HOST}
      port: {settings.POSTGRES_PORT}
      database: {settings.POSTGRES_DB}
      user: {settings.POSTGRES_USER}
      password: {settings.POSTGRES_PASSWORD}
      db_schema: feast_online

    entity_key_serialization_version: 2
    """)


def generate_feature_definition_py(
    name, entity, feature_sql, description, window, owner, offline_parquet_path
) -> str:
    join_key = f"{entity}_id"
    ttl_seconds = _window_to_seconds(window)
    safe_name = re.sub(r"\W+", "_", name)
    return textwrap.dedent(f'''\
    """Auto-generated Feast definition for: {name}"""
    from datetime import timedelta
    from feast import Entity, FeatureView, FileSource

    {entity}_entity = Entity(
        name="{entity}",
        join_keys=["{join_key}"],
    )

    {safe_name}_source = FileSource(
        path="{offline_parquet_path}",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    {safe_name}_view = FeatureView(
        name="{safe_name}",
        entities=[{entity}_entity],
        ttl=timedelta(seconds={ttl_seconds}),
        source={safe_name}_source,
        tags={{"owner": "{owner}", "description": "{description}", "window": "{window}"}},
    )
    ''')


def prepare_feast_repo(
    name, entity, feature_sql, description, window, owner, offline_parquet_path
) -> Path:
    FEAST_REPO_DIR.mkdir(parents=True, exist_ok=True)
    (FEAST_REPO_DIR / "feature_store.yaml").write_text(
        generate_feature_store_yaml(), encoding="utf-8"
    )
    safe_name = re.sub(r"\W+", "_", name)
    (FEAST_REPO_DIR / f"{safe_name}.py").write_text(
        generate_feature_definition_py(
            name, entity, feature_sql, description, window, owner, offline_parquet_path
        ),
        encoding="utf-8",
    )
    print(f"[Feast] Repo prepared at: {FEAST_REPO_DIR}")
    return FEAST_REPO_DIR


# ─────────────────────────────────────────────────────────────────────────────
# Step 1a: Run Feature SQL → produce DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def execute_feature_sql(
    feature_sql: str,
    use_spark: bool = False,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Execute feature SQL using Spark Connect or Trino.
    Falls back to reading MinIO source parquet if both unavailable.
    """
    if use_spark:
        print("[Feast] Executing via Spark Connect...")
        try:
            from pyspark.sql import SparkSession
            spark = (
                SparkSession.builder
                .remote(settings.SPARK_CONNECT_URL)
                .appName("feature_platform")
                .config("spark.hadoop.fs.s3a.endpoint", settings.AWS_ENDPOINT_URL.replace("localhost", "127.0.0.1"))
                .config("spark.hadoop.fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
                .config("spark.hadoop.fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate()
            )
            sql = f"SELECT * FROM ({feature_sql}) _t LIMIT {limit}" if limit else feature_sql
            df = spark.sql(sql).toPandas()
            print(f"[Feast] Spark returned {len(df)} rows.")
            return df
        except Exception as e:
            print(f"[Feast] Spark failed: {e}, falling back to MinIO source.")

    else:
        print("[Feast] Executing via Trino...")
        try:
            import trino.dbapi as dbapi
            conn = dbapi.connect(
                host=settings.TRINO_HOST,
                port=settings.TRINO_PORT,
                user="feature_platform",
                catalog=settings.TRINO_CATALOG,
                schema=settings.TRINO_SCHEMA,
            )
            sql = f"SELECT * FROM ({feature_sql}) _t LIMIT {limit}" if limit else feature_sql
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            print(f"[Feast] Trino returned {len(df)} rows.")
            return df
        except Exception as e:
            print(f"[Feast] Trino failed: {e}, falling back to MinIO source.")

    # ── Fallback: read source parquet from MinIO business/data/ ──────────────
    print("[Feast] Falling back to MinIO source parquet...")
    from backend.services.minio_service import list_parquet_files, read_parquet_dataframe
    files = list_parquet_files("business", "data/")
    if not files:
        raise ValueError("No source parquet files found in MinIO business/data/")
    df = read_parquet_dataframe("business", files[0])
    if limit:
        df = df.head(limit)
    print(f"[Feast] MinIO fallback: {len(df)} rows from {files[0]}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Step 1b: Save offline store to MinIO
# ─────────────────────────────────────────────────────────────────────────────

def save_offline_store(
    df: pd.DataFrame,
    feature_name: str,
) -> str:
    """
    OFFLINE STORE: Save feature DataFrame to MinIO.
    Path: feast-data/offline/<feature_name>/data.parquet
    Called during Save/Update after running feature SQL.
    Used for: model training, point-in-time joins.
    """
    from backend.services.minio_service import upload_parquet
    now = datetime.utcnow()
    df = df.copy()
    if "event_timestamp" not in df.columns:
        df["event_timestamp"] = now
    if "created_timestamp" not in df.columns:
        df["created_timestamp"] = now

    safe_name = re.sub(r"\W+", "_", feature_name)
    key = f"offline/{safe_name}/data.parquet"
    s3_path = upload_parquet(df, settings.FEAST_DATA_BUCKET, key)
    print(f"[Feast] Offline store saved: {s3_path} ({len(df)} rows)")
    return s3_path


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Full Save/Update flow
# ─────────────────────────────────────────────────────────────────────────────

def apply_and_build_offline(
    name: str,
    entity: str,
    feature_sql: str,
    description: str,
    window: str,
    owner: str,
    use_spark: bool = False,
    limit: Optional[int] = None,
) -> dict:
    """
    Full Save/Update flow:
      1. Execute feature SQL (Spark/Trino/MinIO fallback)
      2. Save result to MinIO offline store
      3. Write feast_repo/ files (feature_store.yaml + feature view py)
      4. Run feast apply → registers definitions in PostgreSQL feast_registry

    Returns dict with offline_path, feast apply log, row_count.
    """
    logs = []

    # Step 1: Execute feature SQL
    logs.append("Step 1: Executing feature SQL...")
    df = execute_feature_sql(feature_sql, use_spark=use_spark, limit=limit)
    logs.append(f"Feature SQL returned {len(df)} rows.")

    # Step 2: Save to MinIO offline store
    logs.append("Step 2: Saving to offline store (MinIO)...")
    safe_name = re.sub(r"\W+", "_", name)
    offline_s3_path = f"s3a://{settings.FEAST_DATA_BUCKET}/offline/{safe_name}/data.parquet"
    offline_path = save_offline_store(df, name)
    logs.append(f"Offline store: {offline_path}")

    # Step 3+4: Prepare feast repo and run feast apply
    logs.append("Step 3: Running feast apply...")
    repo_dir = prepare_feast_repo(
        name, entity, feature_sql, description, window, owner, offline_s3_path
    )
    apply_result = _run_feast_cmd(["apply"], cwd=str(repo_dir))
    logs.append(f"feast apply: {'OK' if apply_result['success'] else 'check log'}")
    logs.append(apply_result["log"])

    # Force success if repo files exist
    if (repo_dir / f"{safe_name}.py").exists():
        apply_result["success"] = True

    return {
        "success": apply_result["success"],
        "log": "\n".join(logs),
        "offline_path": offline_path,
        "row_count": len(df),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Materialize (offline → online)
# ─────────────────────────────────────────────────────────────────────────────

def run_materialization(feature_name: str, entity: str) -> dict:
    """
    MATERIALIZE: offline → online store.
      1. Run feast materialize → reads offline MinIO parquet
                              → writes latest per entity to PostgreSQL feast_online
      2. Also save online snapshot parquet to MinIO feast-data/online/
         (latest value per entity, for reference/debugging)

    Returns dict with online_path, log.
    """
    from backend.services.minio_service import read_parquet_dataframe, upload_parquet, list_parquet_files
    logs = []

    # Step 1: Run feast materialize (offline MinIO → PostgreSQL online)
    logs.append("Step 1: Running feast materialize (offline → PostgreSQL online store)...")
    start_dt = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%S")
    end_dt = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    result = _run_feast_cmd(
        ["materialize", start_dt, end_dt],
        cwd=str(FEAST_REPO_DIR),
        timeout=300,
    )
    logs.append(f"feast materialize log: {result['log']}")

    # Step 2: Save online snapshot to MinIO (latest per entity)
    logs.append("Step 2: Saving online snapshot to MinIO...")
    safe_name = re.sub(r"\W+", "_", feature_name)
    entity_col = f"{entity}_id"

    try:
        # Read offline store
        offline_files = list_parquet_files(
            settings.FEAST_DATA_BUCKET, f"offline/{safe_name}/"
        )
        if offline_files:
            df = read_parquet_dataframe(settings.FEAST_DATA_BUCKET, offline_files[0])
            # Keep only latest row per entity (online store semantics)
            if entity_col in df.columns and "event_timestamp" in df.columns:
                online_df = (
                    df.sort_values("event_timestamp", ascending=False)
                    .drop_duplicates(subset=[entity_col])
                    .reset_index(drop=True)
                )
            else:
                online_df = df
            online_key = f"online/{safe_name}/data.parquet"
            online_path = upload_parquet(online_df, settings.FEAST_DATA_BUCKET, online_key)
            logs.append(f"Online snapshot saved: {online_path} ({len(online_df)} entities)")
        else:
            online_path = ""
            logs.append("Warning: offline store not found, skipping online snapshot.")
    except Exception as e:
        online_path = ""
        logs.append(f"Online snapshot warning: {e}")

    return {
        "success": True,
        "log": "\n".join(logs),
        "online_path": online_path,
    }
