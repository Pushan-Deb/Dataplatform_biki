# OpenMetadata on Docker Desktop

This stack runs OpenMetadata as its own separate service boundary with:

- OpenMetadata `1.12.3`
- PostgreSQL as the internal metadata database
- Elasticsearch as the internal search backend
- ZeroTier-only host binding on `10.155.38.139:8585`

## 1. Files

- `docker-compose.yml` contains only the OpenMetadata stack
- `.env` holds the bind address, credentials, and runtime settings
- Docker-managed volumes store PostgreSQL and Elasticsearch data

This stack is independent from the Airflow stack in the parent folder.

## 2. Configure

Copy the example file if needed:

```powershell
Copy-Item .env.example .env
```

Then review at least:

- `OPENMETADATA_BIND_IP`
- `OPENMETADATA_PORT`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `FERNET_KEY`

Note: this stack uses `postgres:16` for the OpenMetadata application database, with `openmetadata_user / openmetadata_password` as the internal database credentials.

If the ZeroTier IP changes, update `OPENMETADATA_BIND_IP` and `OPENMETADATA_SERVER_URL`.

## 3. Start

```powershell
docker compose up -d
```

OpenMetadata should be reachable at:

```text
http://10.155.38.139:8585
```

API login on this stack is email-based:

```text
Email: dataplatform@open-metadata.org
Password: Dataplatform@123
```

## 4. Airflow integration

This install keeps `PIPELINE_SERVICE_CLIENT_ENABLED=false` so OpenMetadata starts as a standalone service first.

Later, when you want OpenMetadata to use your separate Airflow instance, enable the pipeline client and point `PIPELINE_SERVICE_CLIENT_ENDPOINT` to the Airflow endpoint that OpenMetadata should call.

## 5. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all OpenMetadata data:

```powershell
docker compose down -v
```
