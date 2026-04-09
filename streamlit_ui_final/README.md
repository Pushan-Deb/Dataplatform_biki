# Data Platform Console – Streamlit UI

## Service Endpoints

| Service | URL |
|---|---|
| Keycloak | http://10.155.38.139:8090 |
| Airflow | http://10.155.38.139:8080 |
| OpenMetadata | http://10.155.38.139:8585 |
| Marquez UI | http://10.155.38.139:3000 |
| Vault | http://10.155.38.155:8200 |
| MinIO | http://10.155.38.155:9001 |
| Airbyte UI | http://10.155.38.206:8000 |
| Airbyte API | http://10.155.38.206:8001 |
| Spark UI | http://10.155.38.206:8080 |
| Spark Master | spark://10.155.38.206:7077 |
| Spark Custom API | http://10.155.38.206:9003 |
| Spark REST API | http://10.155.38.206:6066 |
| Trino | http://10.155.38.206:8090 |
| Kafka | 10.155.38.206:9092 |

## Credentials
- Username: `dataplatform`
- Password: `Dataplatform@123`

## Run
```bash
pip install -r requirements.txt
streamlit run app.py
```
