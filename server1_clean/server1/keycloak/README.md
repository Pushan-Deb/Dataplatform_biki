# Keycloak on Docker Desktop

This stack runs Keycloak as its own separate service boundary with:

- Keycloak `26.5.5`
- PostgreSQL as Keycloak's internal database
- Caddy as HTTPS reverse proxy (for Microsoft OIDC / Azure AD compatibility)
- ZeroTier-only host binding on `10.155.38.139`

## 1. Files

- `docker-compose.yml` — Keycloak + Postgres + Caddy stack
- `Caddyfile` — Caddy reverse proxy config (auto self-signed TLS)
- `.env` — bind address, ports, credentials, Microsoft OIDC settings
- `data/` — PostgreSQL data and Caddy TLS data
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
- `MICROSOFT_CLIENT_SECRET` (from Azure → Certificates & secrets)

If the ZeroTier IP changes, update `KEYCLOAK_BIND_IP` and `KEYCLOAK_HOSTNAME` and the `Caddyfile`.

## 3. Start

```powershell
docker compose up -d
```

Useful endpoints:

- Keycloak admin console (via Caddy HTTPS): `https://10.155.38.139:8443`
- Keycloak admin console (direct HTTP): `http://10.155.38.139:8090`
- Keycloak health: internal management port `9000`

> **Browser warning**: The first time you open `https://10.155.38.139:8443` you will see a certificate warning because Caddy uses a self-signed internal CA. Trust the cert (see Section 6) to remove the warning.

## 4. Microsoft Entra ID OIDC Setup

### Step A — Azure App Registration (already done)

The app `Keycloak-ckuens` is registered with:
- **Application (Client) ID**: `bb9a553b-3973-4fa7-8251-3df4b8710847`
- **Directory (Tenant) ID**: `7a868a2c-2d6a-470b-aa53-e03cfe747558`

Add this redirect URI in Azure Portal → App registrations → Keycloak-ckuens → Authentication:
```
https://10.155.38.139:8443/realms/ckuens-platform/broker/microsoft/endpoint
```

Get the client secret from: Azure Portal → Keycloak-ckuens → Certificates & secrets → copy the **Value**.

### Step B — Keycloak Identity Provider configuration

1. Open Keycloak admin: `https://10.155.38.139:8443`
2. Select realm: `ckuens-platform`
3. Go to: **Identity Providers → Add provider → Microsoft**
4. Fill in:
   - **Alias**: `microsoft`
   - **Client ID**: `bb9a553b-3973-4fa7-8251-3df4b8710847`
   - **Client Secret**: (value from Azure)
5. Click **Save**

### Step C — Streamlit UI

The Streamlit `app.py` uses `kc_idp_hint=microsoft` to show a "Login with Microsoft" button that bypasses the Keycloak login form and goes directly to Microsoft login.

Set in `.env`:
```dotenv
KEYCLOAK_SERVER=https://10.155.38.139:8443
MICROSOFT_IDP_ALIAS=microsoft
```

## 5. Trust the Caddy self-signed certificate (Windows)

Run once on each machine that accesses the platform:

```powershell
# Extract Caddy root CA from container
docker cp keycloak-caddy:/data/caddy/pki/authorities/local/root.crt caddy-root.crt

# Trust it system-wide (run as Administrator)
certutil -addstore -f "ROOT" caddy-root.crt
```

After this, browsers on that machine will no longer show certificate warnings for `https://10.155.38.139:8443`.

## 6. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all data:

```powershell
docker compose down -v
Remove-Item -Recurse -Force .\data
Remove-Item -Recurse -Force .\config
```
