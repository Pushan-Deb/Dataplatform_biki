# Spark

This service folder contains the Spark standalone cluster for this workspace.

Compose file:
- `C:\Users\CantorCkuens\Downloads\server3\spark\docker-compose.yml`

Main ports:
- `7077` for Spark master
- `6066` for Spark standalone REST submission
- `8080` for Spark master UI
- `8081` for Spark worker UI
- `15002` for Spark Connect
- `4040` for Spark Connect UI

## Start it

In PowerShell:

```powershell
cd C:\Users\CantorCkuens\Downloads\server3\spark
docker compose up -d
```

## Stop it

```powershell
cd C:\Users\CantorCkuens\Downloads\server3\spark
docker compose down
```

## Local access

- `http://localhost:8080`
- `http://localhost:8081`
- `spark://localhost:7077`
- `http://localhost:6066`
- `sc://localhost:15002`

## Remote access over ZeroTier

Use the host ZeroTier IP:

- `http://10.155.38.206:8080`
- `http://10.155.38.206:8081`
- `spark://10.155.38.206:7077`
- `http://10.155.38.206:6066`
- `sc://10.155.38.206:15002`

## Notes

- Worker sizing is configured with `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY` in `.env`.
- The live custom Spark API is managed separately from `C:\spark-api`.
- `legacy-api-placeholder` only contains the old placeholder API that used to live at the repo root.
