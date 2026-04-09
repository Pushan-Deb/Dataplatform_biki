# Service Workspace

Each infrastructure service in this workspace is now structured as its own folder with its own Docker Compose file.

Service folders:

- `spark/`
- `kafka/`
- `trino/`
- `airbyte/`
- `ollama/`

Current compose locations:

- Spark: `C:\Users\CantorCkuens\Downloads\server3\spark\docker-compose.yml`
- Kafka: `C:\Users\CantorCkuens\Downloads\server3\kafka\docker-compose.yml`
- Trino: `C:\Users\CantorCkuens\Downloads\server3\trino\docker-compose.yml`
- Airbyte: `C:\Users\CantorCkuens\Downloads\server3\airbyte\docker-compose.yml`
- Ollama: `C:\Users\CantorCkuens\Downloads\server3\ollama\docker-compose.yml`

Notes:

- The live custom Spark API is managed separately from `C:\spark-api`.
- The `spark/legacy-api-placeholder/` folder only keeps the old placeholder API for reference.

To start a service, change into that service folder and run:

```powershell
docker compose up -d
```
