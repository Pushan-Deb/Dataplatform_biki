# MLflow on Docker Desktop

This stack installs MLflow as a separate service boundary with:

- MLflow tracking server and model registry
- PostgreSQL as the backend store
- MinIO as the artifact store
- Nginx gateway with HTTP basic auth
- ZeroTier-only host binding

## 1. Files

- `docker-compose.yml` contains only the MLflow stack
- `.env` holds the bind address, ports, and credentials
- `nginx/nginx.conf` holds the auth gateway and reverse proxy rules
- `data/` stores persistent PostgreSQL, MinIO, and MLflow state

## 2. Configure

Copy the example file if needed:

```powershell
Copy-Item .env.example .env
```

Review at least:

- `MLFLOW_BIND_IP`
- `MLFLOW_PORT`
- `MLFLOW_ALLOWED_HOSTS`

If the ZeroTier IP changes, update `MLFLOW_BIND_IP` and `MLFLOW_ALLOWED_HOSTS`.

## 3. Start

```powershell
docker compose up -d
```

Useful endpoint:

- MLflow UI and API: `http://10.155.38.139:5002`

Login for the gateway:

- Username: `dataplatform`
- Password: `Dataplatform@123`

## 4. Notes

Artifacts are stored in the internal MinIO bucket `mlflow-artifacts`.

This stack exposes only the MLflow gateway on the host. PostgreSQL and MinIO remain private to the MLflow compose network.

## 5. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all MLflow data:

```powershell
docker compose down -v
Remove-Item -Recurse -Force .\data
```
