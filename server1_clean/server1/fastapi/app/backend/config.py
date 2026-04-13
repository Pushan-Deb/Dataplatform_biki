"""
config.py - Central settings loaded from .env
env_file values take priority over OS environment variables.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # env_file takes priority over OS environment variables
        env_nested_delimiter="__",
    )

    # Ports
    FASTAPI_PORT: int = 8000
    MINIO_PORT: int = 9000
    MLFLOW_PORT: int = 5050

    # MinIO / S3
    AWS_ACCESS_KEY_ID: str = "admin"
    AWS_SECRET_ACCESS_KEY: str = "admin12345"
    AWS_DEFAULT_REGION: str = "us-east-1"
    AWS_ENDPOINT_URL: str = "http://127.0.0.1:9000"
    AWS_EC2_METADATA_DISABLED: str = "true"

    MINIO_ROOT_USER: str = "admin"
    MINIO_ROOT_PASSWORD: str = "admin12345"

    FEAST_DEFINITIONS_BUCKET: str = "feast-definitions"
    FEAST_DATA_BUCKET: str = "feast-data"

    # PostgreSQL
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "feature_db"
    POSTGRES_USER: str = "feature_user"
    POSTGRES_PASSWORD: str = "feature_pass"
    POSTGRES_DEFAULT_SCHEMA: str = "public"
    DATABASE_URL: str = "postgresql+psycopg2://feature_user:feature_pass@localhost:5432/feature_db"

    # Feast Registry
    FEAST_REGISTRY_DB: str = "feast_registry"
    FEAST_REGISTRY_URL: str = "postgresql://feature_user:feature_pass@localhost:5432/feast_registry"

    # MLflow
    MLFLOW_TRACKING_URI: str = "http://localhost:5050"
    MLFLOW_S3_ENDPOINT_URL: str = "http://127.0.0.1:9000"
    MLFLOW_ARTIFACT_BUCKET: str = "mlflow-artifacts"

    # App
    PROJECT_NAME: str = "feature_platform"
    API_BASE_URL: str = "http://localhost:8000"
    LOCAL_STATE_DIR: str = "./state"
    LOCAL_FEAST_REPO_DIR: str = "./feast_repo"

    # Trino
    TRINO_HOST: str = "localhost"
    TRINO_PORT: int = 8080
    TRINO_CATALOG: str = "hive"
    TRINO_SCHEMA: str = "business"

    # Spark Connect
    SPARK_CONNECT_URL: str = "sc://localhost:15002"

    JOB_POLL_SECONDS: int = 2


def get_settings() -> Settings:
    """Always read fresh from .env file, ignoring OS environment variables."""
    import os
    from pathlib import Path

    # Explicitly read .env and override OS env vars
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ[key.strip()] = value.strip()

    return Settings()
