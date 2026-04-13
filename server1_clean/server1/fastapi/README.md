# FastAPI Feature Platform

This stack installs the recovered FastAPI backend from `streamlit_ui_merged.zip` with the supporting services it expects:

- FastAPI backend
- PostgreSQL
- MinIO
- MLflow

Host endpoint:

- FastAPI: `http://10.155.38.139:8000`

Start:

```powershell
docker compose up -d
```

Stop:

```powershell
docker compose down
```

Notes:

- The source archive was partially damaged, so this stack includes reconstructed `mlflow_service.py` and `trino_service.py`.
- The Streamlit UI was not installed here; this stack only brings up the FastAPI backend and its required dependencies.
- The first-pass install excludes the heaviest optional Python extras (`feast`, `trino`, and the in-app MLflow client package), so the backend can boot cleanly. Endpoints that rely on those libraries can be added back after the base service is up.
