"""
backend/main.py - FastAPI entry point for Feature Platform.

Start from streamlit_ui_merged root:
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.config import get_settings

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[Startup] Initialising database tables...")
    try:
        from backend.models import init_db
        init_db()
    except Exception as e:
        print(f"[Startup] Feature DB init warning: {e}")
    try:
        from backend.models_db import init_models_db
        init_models_db()
    except Exception as e:
        print(f"[Startup] Model DB init warning: {e}")
    print("[Startup] Pre-creating MLflow experiments...")
    try:
        from backend.services.mlflow_service import ensure_experiments
        ensure_experiments()
    except Exception as e:
        print(f"[Startup] MLflow experiments warning (non-fatal): {e}")
    yield
    print("[Shutdown] Feature Platform API stopped.")


app = FastAPI(
    title="Feature Platform API",
    description="ML Engineer feature definition, materialisation and model training platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from backend.routers.features import router as features_router
from backend.routers.models import router as models_router

app.include_router(features_router)
app.include_router(models_router)


@app.get("/health")
def health_check():
    return {"status": "ok", "project": settings.PROJECT_NAME}


@app.get("/debug-minio")
def debug_minio():
    import boto3
    from botocore.client import Config
    s = get_settings()
    endpoint = s.AWS_ENDPOINT_URL.replace("localhost", "127.0.0.1")
    try:
        client = boto3.client(
            "s3", endpoint_url=endpoint,
            aws_access_key_id=s.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=s.AWS_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name=s.AWS_DEFAULT_REGION,
        )
        buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
        return {"endpoint": endpoint, "buckets": buckets, "status": "ok"}
    except Exception as e:
        return {"endpoint": endpoint, "error": str(e)}
