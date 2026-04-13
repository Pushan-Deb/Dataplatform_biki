# Airflow on Docker Desktop

This stack gives you:

- Apache Airflow 3.1.8 with CeleryExecutor
- PostgreSQL and Redis as Airflow's internal dependencies
- A ZeroTier-only access model that mirrors your future separate-server deployment pattern

## 1. Prepare Docker Desktop

On this machine, `docker`, `docker compose`, and WSL 2 are already installed.

Before you run the stack:

1. Open Docker Desktop and make sure it is running.
2. In Docker Desktop settings, allocate at least 4 GB RAM.
3. Review `.env`.
4. Replace:
   - `AIRFLOW_ADMIN_USERNAME`
   - `AIRFLOW_ADMIN_PASSWORD`
   - `AIRFLOW_FERNET_KEY`
   - `AIRFLOW_SECRET_KEY`
5. Confirm the ZeroTier bind values:
   - `AIRFLOW_HOST_BIND=10.155.38.139`
   - `AIRFLOW_PORT=8080`
   - `AIRFLOW_BASE_URL=http://10.155.38.139:8080`
   - `AIRFLOW_API_BASE_URL=http://10.155.38.139:8080`
6. If the ZeroTier IP changes later, update those values before starting the stack again.

PowerShell example for generating secrets:

```powershell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
```

## 2. Start the stack

Initialize Airflow:

```powershell
docker compose --profile init up airflow-init
```

Start everything:

```powershell
docker compose up -d
```

Useful URLs:

- Airflow UI/API: `http://10.155.38.139:8080`

Default Airflow username is whatever you set in `.env` as `AIRFLOW_ADMIN_USERNAME`.
The init step writes the matching Airflow 3 simple-auth password file into `config/simple_auth_manager_passwords.json`.

## 3. ZeroTier access

Other devices on the same ZeroTier network can open:

```text
http://10.155.38.139:8080
```

To confirm the current ZeroTier IP on this Windows machine:

```powershell
Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -like "ZeroTier*" }
```

If clients still cannot connect, allow inbound TCP 8080 for the ZeroTier adapter in Windows Defender Firewall.

## 4. Security model

This stack is meant to be reachable through ZeroTier only.

The Airflow port is bound to the machine's ZeroTier address instead of `0.0.0.0`, which keeps it off the normal LAN interface by default.

If you later move to a separate client server, keep the same pattern:

1. Install Airflow on its own host.
2. Join that host to ZeroTier.
3. Bind Airflow to that host's ZeroTier IP.
4. Point your platform to the ZeroTier endpoint.

## 5. Stop or reset

Stop:

```powershell
docker compose down
```

Reset everything including the Airflow database:

```powershell
docker compose down -v
```
