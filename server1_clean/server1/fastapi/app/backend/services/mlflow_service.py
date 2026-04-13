"""
services/mlflow_service.py - MLflow integration helpers.
"""
import json
import os
from datetime import datetime

from backend.config import get_settings

settings = get_settings()

os.environ["MLFLOW_TRACKING_URI"] = settings.MLFLOW_TRACKING_URI
os.environ["MLFLOW_S3_ENDPOINT_URL"] = settings.MLFLOW_S3_ENDPOINT_URL
os.environ["AWS_ACCESS_KEY_ID"] = settings.AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = settings.AWS_SECRET_ACCESS_KEY

EXPERIMENT_FEATURE_DEFS = "feature_definitions"
EXPERIMENT_MATERIALIZATIONS = "feature_materializations"
EXPERIMENT_MODEL_TRAINING = "model_training"
ALL_EXPERIMENTS = [
    EXPERIMENT_FEATURE_DEFS,
    EXPERIMENT_MATERIALIZATIONS,
    EXPERIMENT_MODEL_TRAINING,
]


def _mlflow():
    import mlflow

    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
    return mlflow


def _get_or_create_experiment(name: str) -> str:
    mlflow = _mlflow()
    exp = mlflow.get_experiment_by_name(name)
    if exp is None:
      return mlflow.create_experiment(
          name,
          artifact_location=f"s3://{settings.MLFLOW_ARTIFACT_BUCKET}/{name}",
      )
    return exp.experiment_id


def ensure_experiments():
    try:
        for exp_name in ALL_EXPERIMENTS:
            _get_or_create_experiment(exp_name)
    except Exception as exc:
        print(f"[MLflow] ensure_experiments warning: {exc}")


def log_feature_definition(
    name,
    entity,
    feature_sql,
    description,
    window,
    refresh_cadence,
    owner,
    source_datasets,
    feast_apply_log,
    version,
    action="save",
):
    try:
        mlflow = _mlflow()
        exp_id = _get_or_create_experiment(EXPERIMENT_FEATURE_DEFS)
        with mlflow.start_run(experiment_id=exp_id, run_name=f"{name}_v{version}_{action}") as run:
            mlflow.set_tags(
                {
                    "feature_name": name,
                    "entity": entity,
                    "owner": owner,
                    "action": action,
                    "version": str(version),
                }
            )
            mlflow.log_params(
                {
                    "feature_name": name,
                    "entity": entity,
                    "window": window,
                    "refresh_cadence": refresh_cadence,
                    "owner": owner,
                    "version": version,
                    "source_datasets": json.dumps(source_datasets),
                }
            )
            mlflow.log_text(feature_sql, "feature_sql.sql")
            mlflow.log_text(description or "", "description.txt")
            mlflow.log_text(feast_apply_log or "", "feast_apply.log")
            return run.info.run_id
    except Exception as exc:
        print(f"[MLflow] log_feature_definition warning: {exc}")
        return None


def log_materialization(feature_name, offline_path, online_path, row_count, duration_seconds, status, log_text):
    try:
        mlflow = _mlflow()
        exp_id = _get_or_create_experiment(EXPERIMENT_MATERIALIZATIONS)
        run_name = f"{feature_name}_materialize_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
            mlflow.set_tags({"feature_name": feature_name, "status": status})
            mlflow.log_params(
                {
                    "feature_name": feature_name,
                    "offline_path": offline_path,
                    "online_path": online_path,
                }
            )
            mlflow.log_metrics(
                {
                    "row_count": row_count,
                    "duration_seconds": duration_seconds,
                }
            )
            mlflow.log_text(log_text or "", "materialization.log")
            return run.info.run_id
    except Exception as exc:
        print(f"[MLflow] log_materialization warning: {exc}")
        return None


def get_feature_run_history(feature_name: str) -> list:
    try:
        mlflow = _mlflow()
        exp = mlflow.get_experiment_by_name(EXPERIMENT_FEATURE_DEFS)
        if exp is None:
            return []
        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            filter_string=f"tags.feature_name = '{feature_name}'",
            order_by=["start_time DESC"],
            max_results=50,
        )
        return runs.to_dict("records") if not runs.empty else []
    except Exception as exc:
        print(f"[MLflow] get_feature_run_history warning: {exc}")
        return []


def log_model_training(
    model_name,
    algorithm,
    training_dataset,
    feature_set,
    label_column,
    stage,
    hyperparameters,
    metrics,
    training_data_path,
    model_artifact_path,
    description,
    owner,
):
    try:
        mlflow = _mlflow()
        exp_id = _get_or_create_experiment(EXPERIMENT_MODEL_TRAINING)
        run_name = f"{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
            mlflow.set_tags(
                {
                    "model_name": model_name,
                    "algorithm": algorithm,
                    "stage": stage,
                    "owner": owner,
                }
            )
            params = {
                "model_name": model_name,
                "algorithm": algorithm,
                "training_dataset": training_dataset,
                "feature_set": json.dumps(feature_set),
                "label_column": label_column,
                "description": description,
                "training_data_path": training_data_path,
                "model_artifact_path": model_artifact_path,
            }
            params.update({f"hp_{k}": str(v) for k, v in hyperparameters.items()})
            mlflow.log_params(params)
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, (int, float))})
            mlflow.log_dict(
                {
                    "feature_set": feature_set,
                    "metrics": metrics,
                    "hyperparameters": hyperparameters,
                },
                "training_summary.json",
            )
            return run.info.run_id
    except Exception as exc:
        print(f"[MLflow] log_model_training warning: {exc}")
        return None


def log_model_promotion(model_name, old_stage, new_stage, note, run_id):
    try:
        mlflow = _mlflow()
        exp_id = _get_or_create_experiment(EXPERIMENT_MODEL_TRAINING)
        with mlflow.start_run(experiment_id=exp_id, run_name=f"{model_name}_promotion_{run_id}"):
            mlflow.set_tags(
                {
                    "model_name": model_name,
                    "action": "promotion",
                    "old_stage": old_stage,
                    "new_stage": new_stage,
                }
            )
            mlflow.log_params(
                {
                    "model_name": model_name,
                    "old_stage": old_stage,
                    "new_stage": new_stage,
                    "run_id": run_id,
                }
            )
            mlflow.log_text(note or "", "promotion_note.txt")
    except Exception as exc:
        print(f"[MLflow] log_model_promotion warning: {exc}")

