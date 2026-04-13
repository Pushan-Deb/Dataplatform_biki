import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from the project root (one level above ui/)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

# ==============================
# SERVICE URLS & CREDENTIALS
# ==============================

# Keycloak
KEYCLOAK_SERVER = os.getenv("KEYCLOAK_SERVER", "http://localhost:8090")
REALM = os.getenv("KEYCLOAK_REALM", "master")
CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "data-platform-ui")
CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("KEYCLOAK_REDIRECT_URI", "http://localhost:8501")

# Airflow
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_API = os.getenv("AIRFLOW_API", "http://localhost:8080/api/v2")

# OpenMetadata
OPENMETADATA_URL = os.getenv("OPENMETADATA_URL", "http://localhost:8585")
OPENMETADATA_API = os.getenv("OPENMETADATA_API", "http://localhost:8585/api/v1")

# Marquez / OpenLineage
MARQUEZ_UI = os.getenv("MARQUEZ_UI", "http://localhost:3000")
MARQUEZ_API = os.getenv("MARQUEZ_API", "http://localhost:5000")

# Vault
VAULT_URL = os.getenv("VAULT_URL", "http://localhost:8200")

# MinIO
MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9001")

# Airbyte
AIRBYTE_UI = os.getenv("AIRBYTE_UI", "http://localhost:8000")
AIRBYTE_API = os.getenv("AIRBYTE_API", "http://localhost:8001")

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://localhost:7077")
SPARK_UI = os.getenv("SPARK_UI", "http://localhost:8080")
SPARK_CUSTOM_API = os.getenv("SPARK_CUSTOM_API", "http://localhost:9003")
SPARK_REST_API = os.getenv("SPARK_REST_API", "http://localhost:6066")

# Trino
TRINO_URL = os.getenv("TRINO_URL", "http://localhost:8090")

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Health API
HEALTH_API = os.getenv("HEALTH_API", "http://localhost:8005/api/health")

# Grafana
GRAFANA_KAFKA = os.getenv("GRAFANA_KAFKA", "http://localhost:3000")
GRAFANA_MINIO = os.getenv("GRAFANA_MINIO", "http://localhost:3000")

# Shared credentials
PLATFORM_USERNAME = os.getenv("PLATFORM_USERNAME", "dataplatform")
PLATFORM_PASSWORD = os.getenv("PLATFORM_PASSWORD", "")

# Microsoft OIDC IDP (via Keycloak broker)
MICROSOFT_IDP_ALIAS = os.getenv("MICROSOFT_IDP_ALIAS", "microsoft")

ROLE_TEAMS = {
    "Admin": "Platform",
    "Data Engineer": "DataEng",
    "Data Analyst": "Analytics",
    "ML Engineer": "ML",
}
