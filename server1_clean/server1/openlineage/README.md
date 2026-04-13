# OpenLineage on Docker Desktop

This stack installs OpenLineage using Marquez as a separate service boundary with:

- Marquez API
- Marquez Web UI
- PostgreSQL as the internal metadata database
- Nginx gateway with HTTP basic auth
- ZeroTier-only host binding

## 1. Files

- `docker-compose.yml` contains only the OpenLineage stack
- `.env` holds the bind address, ports, and database passwords
- `marquez.yml` holds the Marquez server configuration
- `nginx/nginx.conf` holds the auth gateway and reverse proxy rules
- `init-db.sh` initializes the Marquez database
- `data/` stores persistent PostgreSQL state

## 2. Configure

Copy the example file if needed:

```powershell
Copy-Item .env.example .env
```

Review at least:

- `OPENLINEAGE_BIND_IP`
- `MARQUEZ_API_PORT`
- `MARQUEZ_ADMIN_PORT`
- `MARQUEZ_WEB_PORT`
- `POSTGRES_PASSWORD`
- `POSTGRES_ROOT_PASSWORD`

If the ZeroTier IP changes, update `OPENLINEAGE_BIND_IP`.

## 3. Start

```powershell
docker compose up -d
```

Useful endpoints:

- Marquez Web UI: `http://10.155.38.139:3000`
- Marquez API: `http://10.155.38.139:5000`
- Marquez admin health: `http://10.155.38.139:5001/healthcheck`
- OpenLineage event endpoint: `http://10.155.38.139:5000/api/v1/lineage`

Credentials for the gateway:

- Username: `dataplatform`
- Password: `Dataplatform@123`

## 4. Notes

This stack keeps search disabled, so it stays lightweight and independent.

If you later want search-backed metadata discovery, we can extend this stack with the extra search service separately.

## 5. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all OpenLineage data:

```powershell
docker compose down -v
Remove-Item -Recurse -Force .\data
```
