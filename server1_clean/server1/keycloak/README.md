# Keycloak on Docker Desktop

This stack runs Keycloak as its own separate service boundary with:

- Keycloak `26.5.5`
- PostgreSQL as Keycloak's internal database
- Caddy as HTTPS reverse proxy (required for Microsoft Entra ID OIDC)
- ZeroTier-only host binding on `10.155.38.139`

## 1. Files

- `docker-compose.yml` — Keycloak + Postgres + Caddy stack
- `Caddyfile` — Caddy reverse proxy config (auto self-signed TLS via `tls internal`)
- `.env` — bind address, ports, credentials, Microsoft OIDC settings
- `data/` — PostgreSQL data and Caddy TLS certificate data
- `config/` — Caddy config persistence

This stack is independent from the other service folders in the parent directory.

## 2. Configure

Copy the example file:

```powershell
Copy-Item .env.example .env
```

Fill in at minimum:

- `KEYCLOAK_ADMIN_PASSWORD`
- `KEYCLOAK_DB_PASSWORD`
- `MICROSOFT_CLIENT_SECRET` (from Azure Portal → Keycloak-ckuens → Certificates & secrets → Value)

If the ZeroTier IP changes, update `KEYCLOAK_BIND_IP`, `KEYCLOAK_HOSTNAME`, and the address in `Caddyfile`.

## 3. Start

```powershell
docker compose up -d
```

Useful endpoints:

| Endpoint | URL |
|----------|-----|
| Keycloak Admin (HTTPS via Caddy) | `https://10.155.38.139:8443` |
| Keycloak Admin (HTTP direct) | `http://10.155.38.139:8090` |
| Keycloak Management/Health | `http://10.155.38.139:9000/health/ready` |

> **Browser warning on first open**: Caddy uses a self-signed internal CA. You will see a certificate warning until you trust the cert (see Section 5).

## 4. Microsoft Entra ID OIDC Setup

### Step A — Azure App Registration

The app `Keycloak-ckuens` is already registered:
- **Application (Client) ID**: `bb9a553b-3973-4fa7-8251-3df4b8710847`
- **Directory (Tenant) ID**: `7a868a2c-2d6a-470b-aa53-e03cfe747558`

In Azure Portal → App registrations → Keycloak-ckuens → **Authentication**, add this redirect URI:

```
https://10.155.38.139:8443/realms/ckuens-platform/broker/microsoft/endpoint
```

Azure will now accept it because it starts with `https://`.

### Step B — Get the Client Secret

Azure Portal → Keycloak-ckuens → **Certificates & secrets** → copy the **Value** of the existing secret.
If the value is hidden (`*****`), create a new secret and copy it immediately.

Set in `.env`:
```dotenv
MICROSOFT_CLIENT_SECRET=<paste value here>
```

### Step C — Configure Keycloak Identity Provider

1. Log in to Keycloak Admin at `https://10.155.38.139:8443`
2. Select realm `ckuens-platform`
3. Go to **Identity Providers** → **Add provider** → **Microsoft**
4. Set **Alias** to `microsoft`
5. Fill in **Client ID** and **Client Secret** from `.env`
6. Save

## 5. Trust the Caddy Self-Signed Certificate (optional)

To avoid browser warnings, export and trust the Caddy root CA:

```powershell
# Copy cert from the running container
docker cp keycloak-caddy:/data/caddy/pki/authorities/local/root.crt caddy-root.crt

# Import into Windows trusted root store
Import-Certificate -FilePath .\caddy-root.crt -CertStoreLocation Cert:\LocalMachine\Root
```

## 6. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all Keycloak data:

```powershell
docker compose down -v
Remove-Item -Recurse -Force .\data
Remove-Item -Recurse -Force .\config
```
