# ==============================
# SERVICE URLS & CREDENTIALS
# ==============================

# Keycloak
KEYCLOAK_SERVER = "http://10.155.38.139:8090"
REALM = "ckuens-platform"
CLIENT_ID = "data-platform-ui"
CLIENT_SECRET = "RPWq4cCj18iD65EWogpsuZWznQ9ORcA2"
REDIRECT_URI = "http://10.155.38.139:8501"

# Airflow
AIRFLOW_URL = "http://10.155.38.139:8080"
AIRFLOW_API = "http://10.155.38.139:8080/api/v2"

# OpenMetadata
OPENMETADATA_URL = "http://10.155.38.139:8585"
OPENMETADATA_API = "http://10.155.38.139:8585/api/v1"

# Marquez / OpenLineage
MARQUEZ_UI = "http://10.155.38.139:3000"
MARQUEZ_API = "http://10.155.38.139:5000"

# Vault
VAULT_URL = "http://10.155.38.155:8200"

# MinIO
MINIO_URL = "http://10.155.38.155:9001"

# Airbyte
AIRBYTE_UI = "http://10.155.38.206:8000"
AIRBYTE_API = "http://10.155.38.206:8001"

# Spark
SPARK_MASTER = "spark://10.155.38.206:7077"
SPARK_UI = "http://10.155.38.206:8080"
SPARK_CUSTOM_API = "http://10.155.38.206:9003"
SPARK_REST_API = "http://10.155.38.206:6066"

# Trino
TRINO_URL = "http://10.155.38.206:8090"

# Kafka
KAFKA_BOOTSTRAP = "10.155.38.206:9092"

# Health API
HEALTH_API = "http://10.155.38.139:8005/api/health"

# Grafana
GRAFANA_KAFKA = "http://10.155.38.139:3000/d/5nhADrDWk/kafka-monitoring?orgId=1&from=now-5m&to=now&refresh=5s"
GRAFANA_MINIO = "http://10.155.38.139:3000/d/TgmJnqnnk/minio-dashboard?orgId=1&from=now-5m&to=now&refresh=5s"

# Shared credentials
PLATFORM_USERNAME = "dataplatform"
PLATFORM_PASSWORD = "Dataplatform@123"

ROLE_TEAMS = {
    "Admin": "Platform",
    "Data Engineer": "DataEng",
    "Data Analyst": "Analytics",
    "ML Engineer": "ML",
}
