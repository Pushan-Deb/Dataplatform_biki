# Keycloak on Docker Desktop

This stack runs Keycloak as its own separate service boundary with:

- Keycloak `26.5.5`
- PostgreSQL as Keycloak's internal database
- ZeroTier-only host binding on `10.155.38.139:8090`

## 1. Files

- `docker-compose.yml` contains only the Keycloak stack
- `.env` holds the bind address, ports, and credentials
- `data/` stores PostgreSQL data

This stack is independent from the other service folders in the parent directory.

## 2. Configure

Copy the example file if needed:

```powershell
Copy-Item .env.example .env
```

Review at least:

- `KEYCLOAK_BIND_IP`
- `KEYCLOAK_HTTP_PORT`
- `KEYCLOAK_HOSTNAME`
- `KEYCLOAK_ADMIN_PASSWORD`
- `KEYCLOAK_DB_PASSWORD`

If the ZeroTier IP changes, update `KEYCLOAK_BIND_IP` and `KEYCLOAK_HOSTNAME`.

## 3. Start

```powershell
docker compose up -d
```

Useful endpoints:

- Keycloak admin console: `http://10.155.38.139:8090`
- Keycloak health: internal container management port `9000`

## 4. Notes

This install uses HTTP because you are exposing it only over ZeroTier for testing.

If you later want a public or production-style setup, we should put Keycloak behind HTTPS and review hostname, proxy, and certificate settings.

## 5. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all Keycloak data:

```powershell
docker compose down -v
Remove-Item -Recurse -Force .\data
```
