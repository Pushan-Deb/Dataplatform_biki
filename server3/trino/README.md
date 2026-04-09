# Trino On Docker Desktop

This stack runs Trino as a separate service from Spark.

## Start

```powershell
cd trino
docker compose up -d
```

## Access

- Local UI/API: `http://localhost:8090`
- ZeroTier UI/API: `http://YOUR_ZT_IP:8090`

## Test

```powershell
docker exec trino trino --execute "SELECT count(*) FROM tpch.sf1.nation;"
```

## Stop

```powershell
cd trino
docker compose down
```

The container listens on port `8080` internally and is published as host port `8090` to avoid conflicting with Spark on the same machine.

