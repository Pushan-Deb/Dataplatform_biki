# Keycloak on Docker Desktop

This stack runs Keycloak as its own separate service boundary with:

- Keycloak `26.5.5`
- PostgreSQL as Keycloak's internal database
- Caddy reverse proxy providing HTTPS on `10.155.38.139:8443` (ZeroTier)
- ZeroTier-only host binding on `10.155.38.139`

## 1. Files

- `docker-compose.yml` contains only the Keycloak stack
- `Caddyfile` configures the Caddy HTTPS reverse proxy
- `.env` holds the bind address, ports, and credentials
- `data/` stores PostgreSQL and Caddy TLS state
- `config/caddy/` stores Caddy runtime config

This stack is independent from the other service folders in the parent directory.

## 2. Configure

Copy the example file if needed:

```powershell
Copy-Item .env.example .env
```

Review at least:

- `KEYCLOAK_BIND_IP`
- `KEYCLOAK_HTTP_PORT`
- `KEYCLOAK_HTTPS_PORT`
- `KEYCLOAK_HOSTNAME`
- `KEYCLOAK_ADMIN_PASSWORD`
- `KEYCLOAK_DB_PASSWORD`
- `MICROSOFT_CLIENT_SECRET`

If the ZeroTier IP changes, update `KEYCLOAK_BIND_IP` and `KEYCLOAK_HOSTNAME`.

## 3. Start

```powershell
docker compose up -d
```

Useful endpoints:

- Keycloak HTTPS (via Caddy): `https://10.155.38.139:8443`
- Keycloak admin console: `https://10.155.38.139:8443/admin`
- Keycloak HTTP (direct, internal): `http://10.155.38.139:8090`
- Keycloak health: internal container management port `9000`

## 4. Caddy HTTPS Setup

Caddy automatically generates a self-signed certificate using its internal CA (`tls internal`). No external domain or certificate authority is needed.

Because the certificate is self-signed, client machines must trust the Caddy root CA to avoid browser warnings.

### Trust the Caddy root CA on Windows (run PowerShell as Administrator)

```powershell
# Copy the Caddy root CA cert from the container
docker cp keycloak-caddy:/data/caddy/pki/authorities/local/root.crt caddy-root.crt

# Add to Windows Trusted Root Certification Authorities
certutil -addstore -f "ROOT" caddy-root.crt
```

After running these commands, restart your browser. The `https://10.155.38.139:8443` address will be trusted without warnings.

## 5. Microsoft Entra ID (Azure) OIDC Setup

Microsoft Azure only accepts HTTPS redirect URIs. The Caddy proxy makes this possible by exposing Keycloak over HTTPS.

### Step 1 — Register the redirect URI in Azure

In Azure Portal → App registrations → `Keycloak-ckuens` → Authentication → Add a platform (Web), add:

```
https://10.155.38.139:8443/realms/ckuens-platform/broker/microsoft/endpoint
```

### Step 2 — Get the client secret

In Azure Portal → App registrations → `Keycloak-ckuens` → Certificates & secrets:

- Copy the **Value** of the existing secret (or create a new one)
- Set it as `MICROSOFT_CLIENT_SECRET` in your `.env` file

### Step 3 — Configure Microsoft IdP in Keycloak

1. Open Keycloak Admin at `https://10.155.38.139:8443/admin`
2. Select realm `ckuens-platform`
3. Go to **Identity Providers** → **Add provider** → **Microsoft**
4. Fill in:
   - **Alias**: `microsoft` (must match `MICROSOFT_IDP_ALIAS` in `.env`)
   - **Client ID**: value of `MICROSOFT_CLIENT_ID` from `.env`
   - **Client Secret**: value of `MICROSOFT_CLIENT_SECRET` from `.env`
5. Click **Save**

### Step 4 — Verify login

Open `https://10.155.38.139:8443/realms/ckuens-platform/account` in a browser. You should see a **Sign in with Microsoft** option on the Keycloak login page.

## 6. Notes

- `MICROSOFT_TENANT_ID` and `MICROSOFT_CLIENT_ID` in `.env.example` are pre-filled with the values from the `Keycloak-ckuens` app registration in the Ckuens Analytics tenant. Update them if the Azure app changes.
- Caddy TLS data is persisted in `./data/caddy` so the self-signed cert survives container restarts.

## 7. Stop or reset

Stop:

```powershell
docker compose down
```

Reset all Keycloak data:

```powershell
docker compose down -v
Remove-Item -Recurse -Force .\data
```
