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

## 6. Microsoft / Entra ID OIDC Setup

Keycloak supports brokering to external Identity Providers (IdPs). Follow the steps below to allow users to **Login with Microsoft** (Azure Active Directory / Entra ID) from the Streamlit UI.

### Step 1 — Register an app in Azure Portal

1. Go to [https://portal.azure.com](https://portal.azure.com) → **Azure Active Directory** → **App registrations** → **New registration**.
2. Give the app a name (e.g. `Keycloak-DataPlatform`).
3. Under **Supported account types**, choose the appropriate option (e.g. *Accounts in this organizational directory only*).
4. Under **Redirect URI**, add:
   ```
   http://10.155.38.139:8090/realms/ckuens-platform/broker/microsoft/endpoint
   ```
5. Click **Register**.
6. On the app overview page, copy the **Application (client) ID** → this is your `MICROSOFT_CLIENT_ID`.
7. Copy the **Directory (tenant) ID** → this is your `MICROSOFT_TENANT_ID`.
8. Go to **Certificates & secrets** → **New client secret**, create one, and copy the value → this is your `MICROSOFT_CLIENT_SECRET`.

### Step 2 — Add the Identity Provider in Keycloak

1. Open the Keycloak admin console: `http://10.155.38.139:8090`
2. Select the realm `ckuens-platform`.
3. Navigate to **Identity Providers** → **Add provider** → **Microsoft**.
4. Fill in:
   - **Alias**: `microsoft` (must match `MICROSOFT_IDP_ALIAS` in `.env`)
   - **Client ID**: the value from `MICROSOFT_CLIENT_ID`
   - **Client Secret**: the value from `MICROSOFT_CLIENT_SECRET`
5. Leave **Tenant** blank to allow all tenants, or enter your tenant ID for single-tenant.
6. Click **Save**.

### Step 3 — Update `.env`

Set the three Microsoft variables in `.env`:

```dotenv
MICROSOFT_TENANT_ID=<your Azure tenant ID>
MICROSOFT_CLIENT_ID=<your Azure app client ID>
MICROSOFT_CLIENT_SECRET=<your Azure app client secret>
MICROSOFT_IDP_ALIAS=microsoft
```

The `MICROSOFT_IDP_ALIAS` value must match the **Alias** you set in the Keycloak admin console (default: `microsoft`).

### How the Streamlit UI uses this

The Streamlit UI shows a **"Login with Microsoft"** button alongside the standard Keycloak login button. When clicked, the button appends `kc_idp_hint=microsoft` to the Keycloak authorization URL. Keycloak detects this hint, skips its own login form, and redirects the user directly to Microsoft's OAuth consent screen.

