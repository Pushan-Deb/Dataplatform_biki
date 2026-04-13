"""
routers/ml_engineer.py - FastAPI router for the ML Engineer page.

Option B Architecture:
  Save/Update  → feast apply + run feature SQL + save to offline store (MinIO)
  Materialize  → feast materialize only (offline MinIO → online PostgreSQL + MinIO)
"""
import time
import threading
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.models import (
    FeatureDefinition, FeatureDefinitionHistory,
    MaterializationJob, get_session_factory,
)
from backend.services import minio_service, trino_service, feast_service, mlflow_service

settings = get_settings()
router = APIRouter(prefix="/features", tags=["Features"])
SessionFactory = get_session_factory()


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class DetectSourcesRequest(BaseModel):
    sql: str


class SaveFeatureRequest(BaseModel):
    name: str = Field(..., description="Feature Definition Name")
    entity: str
    feature_sql: str
    description: str = ""
    window: str = "90d"
    refresh_cadence: str = "Daily"
    owner: str = "ML Engineer"
    source_datasets: list[str] = Field(default_factory=list)
    use_spark: bool = False
    limit: Optional[int] = None


class MaterializeRequest(BaseModel):
    use_spark: bool = False
    limit: Optional[int] = None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _db() -> Session:
    return SessionFactory()


def _snapshot_history(db: Session, defn: FeatureDefinition, note: str = ""):
    last = (
        db.query(FeatureDefinitionHistory)
        .filter_by(definition_id=defn.id)
        .order_by(FeatureDefinitionHistory.version.desc())
        .first()
    )
    next_version = (last.version + 1) if last else 1
    db.add(FeatureDefinitionHistory(
        definition_id=defn.id, version=next_version,
        name=defn.name, entity=defn.entity, feature_sql=defn.feature_sql,
        description=defn.description, window=defn.window,
        refresh_cadence=defn.refresh_cadence, owner=defn.owner,
        source_datasets=defn.source_datasets, change_note=note,
    ))
    return next_version


# ─────────────────────────────────────────────────────────────────────────────
# Background materialisation task
# Option B: feast materialize only (offline → online)
# ─────────────────────────────────────────────────────────────────────────────

def _run_materialization(job_id: int, feature_name: str):
    db = _db()
    job = db.query(MaterializationJob).filter_by(id=job_id).first()
    if not job:
        return

    start_time = time.time()
    logs = []

    try:
        job.status = "running"
        db.commit()

        defn = db.query(FeatureDefinition).filter_by(name=feature_name).first()
        if not defn:
            raise ValueError(f"Feature definition '{feature_name}' not found.")

        logs.append(f"[{datetime.utcnow().isoformat()}] Starting materialisation for '{feature_name}'")
        logs.append("Mode: feast materialize (offline MinIO → online PostgreSQL + MinIO)")

        # ── Materialize: offline → online ─────────────────────────────────────
        result = feast_service.run_materialization(feature_name, defn.entity)
        logs.append(result["log"])
        job.online_path = result.get("online_path", "")

        # offline path is already set from Save/Update
        safe_name = feature_name.replace(" ", "_").replace("-", "_")
        job.offline_path = f"s3://{settings.FEAST_DATA_BUCKET}/offline/{safe_name}/data.parquet"

        # ── Log to MLflow ─────────────────────────────────────────────────────
        duration = time.time() - start_time
        try:
            mlflow_service.log_materialization(
                feature_name=feature_name,
                offline_path=job.offline_path,
                online_path=job.online_path,
                row_count=0,
                duration_seconds=round(duration, 2),
                status="success",
                log_text="\n".join(logs),
            )
        except Exception as mlflow_err:
            logs.append(f"MLflow warning: {mlflow_err}")

        job.status = "success"
        job.finished_at = datetime.utcnow()
        job.log = "\n".join(logs)
        db.commit()

    except Exception as e:
        logs.append(f"ERROR: {e}")
        job.status = "failed"
        job.finished_at = datetime.utcnow()
        job.log = "\n".join(logs)
        db.commit()
        print(f"[Materialisation] FAILED for '{feature_name}': {e}")
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/schema")
def get_business_data_schema():
    """Re-detect SQL: read MinIO business/data/ parquet schema."""
    try:
        files = minio_service.list_parquet_files("business", "data/")
        if not files:
            raise HTTPException(status_code=404, detail="No parquet files found in business/data/")
        schema_info = minio_service.read_parquet_schema("business", files[0])
        schema_info["source_file"] = files[0]
        schema_info["all_files"] = files
        return schema_info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/detect-sources")
def detect_sources(req: DetectSourcesRequest):
    """Parse SQL to extract source tables. Validates via Trino if available."""
    tables = trino_service.detect_source_tables_from_sql(req.sql)
    validation = {"valid": True, "error": None, "plan_summary": ""}
    try:
        validation = trino_service.validate_sql_via_trino(req.sql)
    except Exception as e:
        validation["valid"] = False
        validation["error"] = f"Trino unreachable: {e}"
    return {"detected_tables": tables, "trino_validation": validation}


@router.post("/save-feature")
def save_feature(req: SaveFeatureRequest):
    """
    Save/Update feature definition — Option B:
      1. Execute feature SQL (Spark/Trino/MinIO fallback)
      2. Save result to MinIO offline store (feast-data/offline/)
      3. Write feast_repo/ files
      4. Run feast apply → register in PostgreSQL feast_registry
      5. Save definition to PostgreSQL feature_definitions table
      6. Log to MLflow
    """
    db = _db()
    try:
        existing = db.query(FeatureDefinition).filter_by(name=req.name).first()
        action = "update" if existing else "save"

        # ── Steps 1-4: execute SQL + save offline + feast apply ───────────────
        apply_result = feast_service.apply_and_build_offline(
            name=req.name,
            entity=req.entity,
            feature_sql=req.feature_sql,
            description=req.description,
            window=req.window,
            owner=req.owner,
            use_spark=req.use_spark,
            limit=req.limit,
        )

        # ── Step 5: Save to PostgreSQL ────────────────────────────────────────
        if existing:
            _snapshot_history(db, existing, note="Pre-update snapshot")
            existing.entity = req.entity
            existing.feature_sql = req.feature_sql
            existing.description = req.description
            existing.window = req.window
            existing.refresh_cadence = req.refresh_cadence
            existing.owner = req.owner
            existing.source_datasets = req.source_datasets
            existing.updated_at = datetime.utcnow()
            existing.feast_applied = apply_result["success"]
            existing.feast_apply_log = apply_result["log"]
            defn = existing
        else:
            defn = FeatureDefinition(
                name=req.name, entity=req.entity,
                feature_sql=req.feature_sql, description=req.description,
                window=req.window, refresh_cadence=req.refresh_cadence,
                owner=req.owner, source_datasets=req.source_datasets,
                feast_applied=apply_result["success"],
                feast_apply_log=apply_result["log"],
            )
            db.add(defn)
            db.flush()
            _snapshot_history(db, defn, note="Initial creation")

        db.commit()
        db.refresh(defn)

        last_version = (
            db.query(FeatureDefinitionHistory)
            .filter_by(definition_id=defn.id)
            .order_by(FeatureDefinitionHistory.version.desc())
            .first()
        )
        version = last_version.version if last_version else 1

        # ── Step 6: Log to MLflow ─────────────────────────────────────────────
        mlflow_run_id = None
        try:
            mlflow_run_id = mlflow_service.log_feature_definition(
                name=req.name, entity=req.entity,
                feature_sql=req.feature_sql, description=req.description,
                window=req.window, refresh_cadence=req.refresh_cadence,
                owner=req.owner, source_datasets=req.source_datasets,
                feast_apply_log=apply_result["log"],
                version=version, action=action,
            )
        except Exception as mlflow_err:
            print(f"[MLflow] Warning: {mlflow_err}")

        return {
            "status": "ok",
            "action": action,
            "feature_name": req.name,
            "feast_applied": apply_result["success"],
            "feast_log": apply_result["log"],
            "offline_path": apply_result.get("offline_path", ""),
            "row_count": apply_result.get("row_count", 0),
            "mlflow_run_id": mlflow_run_id,
            "version": version,
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("")
def list_features():
    db = _db()
    try:
        defs = db.query(FeatureDefinition).order_by(FeatureDefinition.updated_at.desc()).all()
        return [
            {
                "id": d.id, "name": d.name, "entity": d.entity,
                "description": d.description, "window": d.window,
                "refresh_cadence": d.refresh_cadence, "owner": d.owner,
                "source_datasets": d.source_datasets,
                "feast_applied": d.feast_applied,
                "created_at": d.created_at.isoformat() if d.created_at else None,
                "updated_at": d.updated_at.isoformat() if d.updated_at else None,
            }
            for d in defs
        ]
    finally:
        db.close()



@router.get("/jobs")
def list_jobs():
    db = _db()
    try:
        jobs = db.query(MaterializationJob).order_by(MaterializationJob.started_at.desc()).all()
        return [
            {
                "id": j.id, "feature_name": j.feature_name, "status": j.status,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "finished_at": j.finished_at.isoformat() if j.finished_at else None,
                "offline_path": j.offline_path, "online_path": j.online_path,
            }
            for j in jobs
        ]
    finally:
        db.close()


@router.get("/jobs/{job_id}")
def get_job(job_id: int):
    db = _db()
    try:
        job = db.query(MaterializationJob).filter_by(id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
        return {
            "id": job.id, "feature_name": job.feature_name, "status": job.status,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            "offline_path": job.offline_path, "online_path": job.online_path,
            "log": job.log,
        }
    finally:
        db.close()

@router.get("/{name}")
def get_feature(name: str):
    db = _db()
    try:
        defn = db.query(FeatureDefinition).filter_by(name=name).first()
        if not defn:
            raise HTTPException(status_code=404, detail=f"Feature '{name}' not found.")
        return {
            "id": defn.id, "name": defn.name, "entity": defn.entity,
            "feature_sql": defn.feature_sql, "description": defn.description,
            "window": defn.window, "refresh_cadence": defn.refresh_cadence,
            "owner": defn.owner, "source_datasets": defn.source_datasets,
            "feast_applied": defn.feast_applied,
            "feast_apply_log": defn.feast_apply_log,
            "created_at": defn.created_at.isoformat() if defn.created_at else None,
            "updated_at": defn.updated_at.isoformat() if defn.updated_at else None,
        }
    finally:
        db.close()


@router.get("/{name}/history")
def get_feature_history(name: str):
    db = _db()
    try:
        defn = db.query(FeatureDefinition).filter_by(name=name).first()
        if not defn:
            raise HTTPException(status_code=404, detail=f"Feature '{name}' not found.")
        history = (
            db.query(FeatureDefinitionHistory)
            .filter_by(definition_id=defn.id)
            .order_by(FeatureDefinitionHistory.version.desc())
            .all()
        )
        return [
            {
                "version": h.version, "name": h.name, "entity": h.entity,
                "feature_sql": h.feature_sql, "description": h.description,
                "window": h.window, "refresh_cadence": h.refresh_cadence,
                "owner": h.owner, "source_datasets": h.source_datasets,
                "snapshot_at": h.snapshot_at.isoformat() if h.snapshot_at else None,
                "change_note": h.change_note,
            }
            for h in history
        ]
    finally:
        db.close()


@router.post("/materialize/{name}")
def trigger_materialization(name: str, req: MaterializeRequest, background_tasks: BackgroundTasks):
    """
    Trigger materialisation — Option B:
      feast materialize only (offline MinIO → online PostgreSQL + MinIO snapshot)
    """
    db = _db()
    try:
        defn = db.query(FeatureDefinition).filter_by(name=name).first()
        if not defn:
            raise HTTPException(status_code=404, detail=f"Feature '{name}' not found.")

        job = MaterializationJob(feature_name=name, status="pending")
        db.add(job)
        db.commit()
        db.refresh(job)
        job_id = job.id

        t = threading.Thread(
            target=_run_materialization,
            args=(job_id, name),
            daemon=True,
        )
        t.start()

        return {"status": "accepted", "job_id": job_id, "feature_name": name}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

