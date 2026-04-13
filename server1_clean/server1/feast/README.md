# Feast on Docker Desktop

This stack installs Feast as its own separate service boundary with:

- Feast feature server
- Feast registry REST server
- PostgreSQL for the Feast registry and online store
- Nginx gateway with HTTP basic auth
- ZeroTier-only host binding

## Endpoints

- Feature server: `http://10.155.38.139:6566`
- Registry REST: `http://10.155.38.139:6572`

Gateway credentials:

- Username: `dataplatform`
- Password: `Dataplatform@123`

## Start

```powershell
docker compose up -d --build
```

## Stop

```powershell
docker compose down
```

## Notes

- This stack seeds a small demo repo and materializes demo data so the service is not empty on first boot.
- PostgreSQL remains private to the compose network.
- If the ZeroTier IP changes, update `FEAST_BIND_IP` in `.env`.
