# Kafka On Docker Desktop

This stack runs Confluent Platform services as a separate service group from Spark and Trino.

## Start

```powershell
cd kafka
docker compose up -d
```

## Access

- Kafka external broker: `10.155.38.206:9092`
- Kafka internal broker on host: `localhost:29092`
- Schema Registry: `http://10.155.38.206:8181`
- Kafka REST Proxy: `http://10.155.38.206:8082`
- Kafka Connect: `http://10.155.38.206:8083`
- Control Center: `http://10.155.38.206:9021`
- Kafka exporter: `http://10.155.38.206:9308/metrics`
- Broker JMX exporter: `http://10.155.38.206:9404/metrics`

Schema Registry uses host port `8181` instead of `8081` because Spark already uses `8081` on this machine.

## Stop

```powershell
cd kafka
docker compose down
```

