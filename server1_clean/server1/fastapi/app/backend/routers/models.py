"""
routers/models.py - FastAPI router for Model training & registry.

Architecture:
  - Models are trained using materialized feature values from offline store (MinIO)
  - Each training run is logged to MLflow experiment 'model_training'
  - Model registry stored in PostgreSQL (model_definitions table)
  - Feature → Model lineage tracked per run

Endpoints:
  POST /models/train                         - Submit model training job
  GET  /models                               - List all models
  GET  /models/lineage                       - Feature → Model lineage across all models
  GET  /models/experiments/list              - List all MLflow experiments with run counts
  GET  /models/experiments/{name}/runs       - Get runs for a named MLflow experiment
  GET  /models/{name}                        - Get model details
  GET  /models/{name}/runs                   - Get training run history for a model
  GET  /models/{name}/runs/{run_id}/poll     - Poll a specific training run
  POST /models/{name}/promote                - Promote stage (Draft → Staging → Production)
  GET  /models/{name}/features               - Get features used by a model
"""
import time
import uuid
import threading
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import get_settings
from backend.models_db import (
    ModelDefinition,
    ModelRunHistory,
    ModelFeatureLineage,
    get_session_factory,
    init_models_db,
)
from backend.services import minio_service, mlflow_service

settings = get_settings()
router = APIRouter(prefix="/models", tags=["Models"])
SessionFactory = get_session_factory()

STAGE_ORDER = ["Draft", "Staging", "Production", "Archived"]


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class TrainModelRequest(BaseModel):
    model_name: str = Field(..., description="Model name")
    algorithm: str = Field(..., description="Algorithm e.g. XGBoost, Logistic Regression")
    training_dataset: str = Field(..., description="Training dataset table or path")
    feature_set: list[str] = Field(..., description="List of feature definition names to use")
    label_column: str = Field(default="label", description="Target/label column name")
    stage: str = Field(default="Draft", description="Draft | Staging | Production")
    hyperparameters: dict = Field(default_factory=dict, description="Model hyperparameters")
    description: str = Field(default="", description="Model description")
    owner: str = Field(default="ML Engineer", description="Model owner")
    use_spark: bool = Field(default=False, description="Use Spark Connect for feature joining")


class PromoteModelRequest(BaseModel):
    stage: str = Field(..., description="Target stage: Staging | Production | Archived")
    note: str = Field(default="", description="Promotion note")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _db() -> Session:
    return SessionFactory()


def _resolve_offline_paths(feature_names: list[str]) -> dict[str, str]:
    """
    Resolve the latest offline store path for each feature from MinIO.
    Returns dict: {feature_name: s3_path}
    """
    resolved = {}
    for fname in feature_names:
        import re
        safe_name = re.sub(r"\W+", "_", fname)
        key = f"offline/{safe_name}/data.parquet"
        try:
            files = minio_service.list_parquet_files(settings.FEAST_DATA_BUCKET, f"offline/{safe_name}/")
            if files:
                resolved[fname] = f"s3://{settings.FEAST_DATA_BUCKET}/{files[0]}"
            else:
                resolved[fname] = "NOT_MATERIALIZED"
        except Exception:
            resolved[fname] = "NOT_MATERIALIZED"
    return resolved


def _build_training_dataframe(feature_names: list[str], label_column: str, limit: Optional[int] = None):
    """
    Build a training DataFrame by joining feature offline parquet files on entity key.
    Falls back to returning source data if features not materialized.
    """
    import pandas as pd
    import re

    dfs = []
    for fname in feature_names:
        safe_name = re.sub(r"\W+", "_", fname)
        try:
            files = minio_service.list_parquet_files(settings.FEAST_DATA_BUCKET, f"offline/{safe_name}/")
            if files:
                df = minio_service.read_parquet_dataframe(settings.FEAST_DATA_BUCKET, files[0])
                # Drop timestamp columns for training
                df = df.drop(columns=["event_timestamp", "created_timestamp"], errors="ignore")
                dfs.append((fname, df))
        except Exception as e:
            print(f"[Models] Could not load offline store for {fname}: {e}")

    if not dfs:
        # Fallback: use business source data
        files = minio_service.list_parquet_files("business", "data/")
        if files:
            df = minio_service.read_parquet_dataframe("business", files[0])
            df = df.drop(columns=["event_timestamp", "created_timestamp"], errors="ignore")
            if limit:
                df = df.head(limit)
            return df, "business/data fallback"
        raise ValueError("No feature data or source data available for training.")

    # Join all feature dataframes on entity key (customer_id, product_id, etc.)
    entity_cols = ["customer_id", "product_id", "order_id", "user_id", "session_id"]
    result_df = None
    entity_col_used = None

    for fname, df in dfs:
        # Find entity key
        for ec in entity_cols:
            if ec in df.columns:
                entity_col_used = ec
                break

        if result_df is None:
            result_df = df
        else:
            # Join on entity key
            if entity_col_used and entity_col_used in result_df.columns and entity_col_used in df.columns:
                # Avoid duplicate columns
                overlap = [c for c in df.columns if c in result_df.columns and c != entity_col_used]
                df = df.drop(columns=overlap, errors="ignore")
                result_df = result_df.merge(df, on=entity_col_used, how="left")
            else:
                # Concatenate columns side by side if no entity key
                result_df = pd.concat([result_df, df], axis=1)

    if limit and result_df is not None:
        result_df = result_df.head(limit)

    return result_df, entity_col_used


# ─────────────────────────────────────────────────────────────────────────────
# Background training task
# ─────────────────────────────────────────────────────────────────────────────

def _run_training(
    run_history_id: int,
    model_name: str,
    algorithm: str,
    training_dataset: str,
    feature_set: list[str],
    label_column: str,
    stage: str,
    hyperparameters: dict,
    description: str,
    owner: str,
):
    db = _db()
    run = db.query(ModelRunHistory).filter_by(id=run_history_id).first()
    if not run:
        return

    start_time = time.time()
    logs = []

    try:
        run.status = "running"
        db.commit()

        logs.append(f"[{datetime.utcnow().isoformat()}] Starting training for '{model_name}'")
        logs.append(f"Algorithm: {algorithm}")
        logs.append(f"Feature set: {feature_set}")
        logs.append(f"Label column: {label_column}")

        # ── Step 1: Resolve offline feature paths ─────────────────────────────
        logs.append("\nStep 1: Resolving offline feature store paths...")
        offline_paths = _resolve_offline_paths(feature_set)
        for fname, path in offline_paths.items():
            logs.append(f"  {fname}: {path}")
            if path == "NOT_MATERIALIZED":
                logs.append(f"  WARNING: {fname} not yet materialized — using source data fallback")

        # ── Step 2: Build training DataFrame ─────────────────────────────────
        logs.append("\nStep 2: Building training dataset from offline store...")
        try:
            train_df, entity_key = _build_training_dataframe(feature_set, label_column)
            logs.append(f"Training dataset shape: {train_df.shape}")
            logs.append(f"Columns: {list(train_df.columns)}")
        except Exception as e:
            raise ValueError(f"Failed to build training dataset: {e}")

        # ── Step 3: Save training dataset snapshot to MinIO ───────────────────
        logs.append("\nStep 3: Saving training dataset snapshot to MinIO...")
        import re
        safe_name = re.sub(r"\W+", "_", model_name)
        run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        training_key = f"models/{safe_name}/training_data/run_{run_ts}/data.parquet"
        training_path = minio_service.upload_parquet(
            train_df, settings.MLFLOW_ARTIFACT_BUCKET, training_key
        )
        logs.append(f"Training data saved: {training_path}")
        run.training_data_path = training_path

        # ── Step 4: Mock model training (metrics simulation) ──────────────────
        logs.append(f"\nStep 4: Training {algorithm} model...")
        # Simulate training metrics based on algorithm and data
        import hashlib
        seed = int(hashlib.md5(f"{model_name}{algorithm}".encode()).hexdigest()[:8], 16) % 1000
        auc = round(0.70 + (seed % 250) / 1000, 4)
        accuracy = round(auc - 0.02 + (seed % 30) / 1000, 4)
        f1 = round(auc - 0.03 + (seed % 20) / 1000, 4)
        precision = round(auc + 0.01 - (seed % 15) / 1000, 4)
        recall = round(auc - 0.04 + (seed % 25) / 1000, 4)

        metrics = {
            "auc": auc,
            "accuracy": accuracy,
            "f1_score": f1,
            "precision": precision,
            "recall": recall,
            "train_rows": len(train_df),
            "feature_count": len(feature_set),
            "duration_seconds": round(time.time() - start_time, 2),
        }
        logs.append(f"Training metrics: {metrics}")

        # ── Step 5: Save model artifact path to MinIO ─────────────────────────
        model_key = f"models/{safe_name}/artifacts/run_{run_ts}/model.pkl"
        model_artifact_path = f"s3://{settings.MLFLOW_ARTIFACT_BUCKET}/{model_key}"
        logs.append(f"\nStep 5: Model artifact path: {model_artifact_path}")
        run.model_artifact_path = model_artifact_path

        # ── Step 6: Log to MLflow ─────────────────────────────────────────────
        logs.append("\nStep 6: Logging to MLflow...")
        try:
            mlflow_run_id = mlflow_service.log_model_training(
                model_name=model_name,
                algorithm=algorithm,
                training_dataset=training_dataset,
                feature_set=feature_set,
                label_column=label_column,
                stage=stage,
                hyperparameters=hyperparameters,
                metrics=metrics,
                training_data_path=training_path,
                model_artifact_path=model_artifact_path,
                description=description,
                owner=owner,
            )
            run.mlflow_run_id = mlflow_run_id
            logs.append(f"MLflow run ID: {mlflow_run_id}")
        except Exception as mlflow_err:
            logs.append(f"MLflow warning: {mlflow_err}")

        # ── Step 7: Update model definition metrics ───────────────────────────
        model_def = db.query(ModelDefinition).filter_by(name=model_name).first()
        if model_def:
            model_def.latest_auc = auc
            model_def.latest_accuracy = accuracy
            model_def.latest_run_id = run.run_id
            model_def.updated_at = datetime.utcnow()

        # ── Complete ──────────────────────────────────────────────────────────
        run.status = "success"
        run.finished_at = datetime.utcnow()
        run.metrics = metrics
        run.log = "\n".join(logs)
        db.commit()
        print(f"[Models] Training completed for '{model_name}' | AUC={auc}")

    except Exception as e:
        logs.append(f"\nERROR: {e}")
        run.status = "failed"
        run.finished_at = datetime.utcnow()
        run.log = "\n".join(logs)
        db.commit()
        print(f"[Models] Training FAILED for '{model_name}': {e}")
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/train")
def train_model(req: TrainModelRequest, background_tasks: BackgroundTasks):
    """
    Submit a model training job.
    1. Resolves offline feature store paths from MinIO
    2. Builds training dataset by joining feature parquets
    3. Trains model (mock metrics for now — plug in real trainer)
    4. Logs all params/metrics to MLflow experiment 'model_training'
    5. Saves model definition + run to PostgreSQL
    6. Tracks Feature → Model lineage
    """
    db = _db()
    try:
        # Validate feature set is not empty
        if not req.feature_set:
            raise HTTPException(status_code=400, detail="feature_set cannot be empty.")

        # Validate stage
        if req.stage not in STAGE_ORDER:
            raise HTTPException(status_code=400, detail=f"stage must be one of {STAGE_ORDER}")

        # Upsert model definition
        existing = db.query(ModelDefinition).filter_by(name=req.model_name).first()
        if existing:
            existing.algorithm = req.algorithm
            existing.training_dataset = req.training_dataset
            existing.feature_set = req.feature_set
            existing.label_column = req.label_column
            existing.stage = req.stage
            existing.hyperparameters = req.hyperparameters
            existing.description = req.description
            existing.owner = req.owner
            existing.updated_at = datetime.utcnow()
            model_def = existing
        else:
            model_def = ModelDefinition(
                name=req.model_name,
                algorithm=req.algorithm,
                training_dataset=req.training_dataset,
                feature_set=req.feature_set,
                label_column=req.label_column,
                stage=req.stage,
                hyperparameters=req.hyperparameters,
                description=req.description,
                owner=req.owner,
            )
            db.add(model_def)
            db.flush()

        # Create run history record
        run_id = f"RUN-{str(uuid.uuid4())[:8].upper()}"
        run = ModelRunHistory(
            model_id=model_def.id,
            model_name=req.model_name,
            run_id=run_id,
            algorithm=req.algorithm,
            feature_set=req.feature_set,
            hyperparameters=req.hyperparameters,
            status="pending",
        )
        db.add(run)
        db.flush()
        run_history_id = run.id

        # Track Feature → Model lineage
        for feat_name in req.feature_set:
            existing_lin = db.query(ModelFeatureLineage).filter_by(
                model_name=req.model_name, feature_name=feat_name
            ).first()
            if not existing_lin:
                db.add(ModelFeatureLineage(
                    model_name=req.model_name,
                    feature_name=feat_name,
                    run_id=run_id,
                ))

        db.commit()

        # Start background training
        t = threading.Thread(
            target=_run_training,
            args=(
                run_history_id, req.model_name, req.algorithm,
                req.training_dataset, req.feature_set, req.label_column,
                req.stage, req.hyperparameters, req.description, req.owner,
            ),
            daemon=True,
        )
        t.start()

        return {
            "status": "accepted",
            "model_name": req.model_name,
            "run_id": run_id,
            "run_history_id": run_history_id,
            "feature_set": req.feature_set,
            "stage": req.stage,
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("")
def list_models():
    """List all model definitions with latest metrics."""
    db = _db()
    try:
        models = db.query(ModelDefinition).order_by(ModelDefinition.updated_at.desc()).all()
        return [
            {
                "id": m.id,
                "name": m.name,
                "algorithm": m.algorithm,
                "training_dataset": m.training_dataset,
                "feature_set": m.feature_set,
                "label_column": m.label_column,
                "stage": m.stage,
                "description": m.description,
                "owner": m.owner,
                "latest_auc": m.latest_auc,
                "latest_accuracy": m.latest_accuracy,
                "latest_run_id": m.latest_run_id,
                "created_at": m.created_at.isoformat() if m.created_at else None,
                "updated_at": m.updated_at.isoformat() if m.updated_at else None,
            }
            for m in models
        ]
    finally:
        db.close()


@router.get("/lineage")
def get_all_lineage():
    """Get Feature → Model lineage across all models."""
    db = _db()
    try:
        lineage = db.query(ModelFeatureLineage).order_by(ModelFeatureLineage.created_at.desc()).all()
        return [
            {
                "model_name": l.model_name,
                "feature_name": l.feature_name,
                "run_id": l.run_id,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in lineage
        ]
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# MLflow Experiment endpoints
# NOTE: These MUST stay above /{name} routes to avoid FastAPI routing conflict.
#       /experiments/list would otherwise be caught by /{name} with name="experiments".
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/experiments/list")
def list_mlflow_experiments():
    """List all MLflow experiments with run counts."""
    try:
        mlflow = mlflow_service._mlflow()
        experiments = mlflow.search_experiments()
        result = []
        for exp in experiments:
            try:
                runs = mlflow.search_runs(
                    experiment_ids=[exp.experiment_id],
                    max_results=1000,
                )
                run_count = len(runs)
                last_run_time = (
                    str(runs["start_time"].max()) if not runs.empty else None
                )
            except Exception:
                run_count = 0
                last_run_time = None
            result.append({
                "experiment_id": exp.experiment_id,
                "name": exp.name,
                "artifact_location": exp.artifact_location,
                "lifecycle_stage": exp.lifecycle_stage,
                "run_count": run_count,
                "last_run_time": last_run_time,
            })
        # Active experiments first, then sorted by run count descending
        result.sort(key=lambda x: (x["lifecycle_stage"] != "active", -x["run_count"]))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MLflow error: {str(e)}")


@router.get("/experiments/{experiment_name}/runs")
def get_experiment_runs(experiment_name: str, limit: int = 50):
    """Get all runs for a named MLflow experiment."""
    try:
        mlflow = mlflow_service._mlflow()
        exp = mlflow.get_experiment_by_name(experiment_name)
        if not exp:
            raise HTTPException(
                status_code=404,
                detail=f"Experiment '{experiment_name}' not found in MLflow."
            )
        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["start_time DESC"],
            max_results=limit,
        )
        if runs.empty:
            return []
        # Flatten column names for clean display
        runs = runs.copy()
        runs.columns = [
            c.replace("tags.", "tag_")
             .replace("params.", "param_")
             .replace("metrics.", "metric_")
            for c in runs.columns
        ]
        # Show most useful columns first, cap total to avoid bloat
        priority = [
            "run_id", "status", "start_time", "end_time",
            "tag_feature_name", "tag_model_name", "tag_algorithm",
            "tag_action", "tag_stage", "tag_owner",
            "metric_auc", "metric_accuracy", "metric_f1_score",
            "metric_row_count", "metric_duration_seconds",
            "param_feature_name", "param_model_name",
        ]
        available = [c for c in priority if c in runs.columns]
        extra = [c for c in runs.columns if c not in priority and not c.startswith("param_hp_")]
        show_cols = available + extra[:5]
        runs = runs[show_cols].fillna("")
        return runs.to_dict("records")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MLflow error: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Per-model routes — /{name} wildcard routes MUST come after all static routes
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/{name}")
def get_model(name: str):
    """Get a single model definition with full details."""
    db = _db()
    try:
        model = db.query(ModelDefinition).filter_by(name=name).first()
        if not model:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found.")
        return {
            "id": model.id,
            "name": model.name,
            "algorithm": model.algorithm,
            "training_dataset": model.training_dataset,
            "feature_set": model.feature_set,
            "label_column": model.label_column,
            "stage": model.stage,
            "hyperparameters": model.hyperparameters,
            "description": model.description,
            "owner": model.owner,
            "latest_auc": model.latest_auc,
            "latest_accuracy": model.latest_accuracy,
            "latest_run_id": model.latest_run_id,
            "created_at": model.created_at.isoformat() if model.created_at else None,
            "updated_at": model.updated_at.isoformat() if model.updated_at else None,
        }
    finally:
        db.close()


@router.get("/{name}/runs")
def get_model_runs(name: str):
    """Get all training run history for a model."""
    db = _db()
    try:
        model = db.query(ModelDefinition).filter_by(name=name).first()
        if not model:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found.")
        runs = (
            db.query(ModelRunHistory)
            .filter_by(model_id=model.id)
            .order_by(ModelRunHistory.started_at.desc())
            .all()
        )
        return [
            {
                "id": r.id,
                "run_id": r.run_id,
                "model_name": r.model_name,
                "algorithm": r.algorithm,
                "feature_set": r.feature_set,
                "hyperparameters": r.hyperparameters,
                "status": r.status,
                "metrics": r.metrics,
                "mlflow_run_id": r.mlflow_run_id,
                "training_data_path": r.training_data_path,
                "model_artifact_path": r.model_artifact_path,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "log": r.log,
            }
            for r in runs
        ]
    finally:
        db.close()


@router.get("/{name}/runs/{run_id}/poll")
def poll_run(name: str, run_id: str):
    """Poll a specific training run status."""
    db = _db()
    try:
        run = db.query(ModelRunHistory).filter_by(run_id=run_id).first()
        if not run:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")
        return {
            "run_id": run.run_id,
            "status": run.status,
            "metrics": run.metrics,
            "mlflow_run_id": run.mlflow_run_id,
            "training_data_path": run.training_data_path,
            "model_artifact_path": run.model_artifact_path,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "log": run.log,
        }
    finally:
        db.close()


@router.post("/{name}/promote")
def promote_model(name: str, req: PromoteModelRequest):
    """
    Promote a model to a new stage.
    Draft → Staging → Production → Archived
    Logs the promotion to MLflow.
    """
    db = _db()
    try:
        model = db.query(ModelDefinition).filter_by(name=name).first()
        if not model:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found.")

        if req.stage not in STAGE_ORDER:
            raise HTTPException(status_code=400, detail=f"stage must be one of {STAGE_ORDER}")

        old_stage = model.stage
        model.stage = req.stage
        model.updated_at = datetime.utcnow()

        # Snapshot current run with promotion note
        run_id = f"PROMO-{str(uuid.uuid4())[:8].upper()}"
        promo_run = ModelRunHistory(
            model_id=model.id,
            model_name=name,
            run_id=run_id,
            algorithm=model.algorithm,
            feature_set=model.feature_set,
            hyperparameters={},
            status="success",
            finished_at=datetime.utcnow(),
            metrics={},
            log=f"Promoted from {old_stage} → {req.stage}. Note: {req.note}",
        )
        db.add(promo_run)
        db.commit()

        # Log to MLflow
        try:
            mlflow_service.log_model_promotion(
                model_name=name,
                old_stage=old_stage,
                new_stage=req.stage,
                note=req.note,
                run_id=run_id,
            )
        except Exception as mlflow_err:
            print(f"[Models] MLflow promotion log warning: {mlflow_err}")

        return {
            "status": "ok",
            "model_name": name,
            "old_stage": old_stage,
            "new_stage": req.stage,
            "run_id": run_id,
            "note": req.note,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/{name}/features")
def get_model_features(name: str):
    """Get all features used by a model with their offline store paths."""
    db = _db()
    try:
        model = db.query(ModelDefinition).filter_by(name=name).first()
        if not model:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found.")

        lineage = db.query(ModelFeatureLineage).filter_by(model_name=name).all()
        offline_paths = _resolve_offline_paths(model.feature_set or [])

        return {
            "model_name": name,
            "feature_set": model.feature_set,
            "offline_paths": offline_paths,
            "lineage": [
                {
                    "feature_name": l.feature_name,
                    "run_id": l.run_id,
                    "offline_path": offline_paths.get(l.feature_name, "unknown"),
                    "created_at": l.created_at.isoformat() if l.created_at else None,
                }
                for l in lineage
            ],
        }
    finally:
        db.close()
