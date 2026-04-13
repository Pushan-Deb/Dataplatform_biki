from __future__ import annotations

import base64
from typing import Any
from urllib.parse import quote

import requests

from platform_env import env_str, require_env
from platform_http import build_session


class SparkAPI:
    def __init__(self) -> None:
        self.base_url = (
            env_str("SPARK_API_URL")
            or env_str("SPARK_REST_URL")
            or env_str("SPARK_MASTER_API_URL")
            or env_str("SPARK_MASTER_URL")
            or "http://spark-master:6066"
        ).rstrip("/")
        self.session = build_session("SPARK")

    def submit_job(
        self,
        app_resource: str,
        app_name: str,
        env_vars: dict[str, str] | None = None,
        app_args: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "action": "CreateSubmissionRequest",
            "appResource": app_resource,
            "appArgs": app_args or [],
            "clientSparkVersion": env_str("SPARK_CLIENT_VERSION", "4.0.1"),
            "environmentVariables": env_vars or {},
            "mainClass": env_str("SPARK_MAIN_CLASS", ""),
            "sparkProperties": {
                "spark.app.name": app_name,
                "spark.master": env_str("SPARK_MASTER", "spark://spark-master:7077"),
                "spark.submit.deployMode": env_str("SPARK_DEPLOY_MODE", "cluster"),
            },
        }
        response = self.session.post(f"{self.base_url}/v1/submissions/create", json=payload)
        response.raise_for_status()
        return response.json()

    def get_job_status(self, submission_id: str) -> dict[str, Any]:
        response = self.session.get(f"{self.base_url}/v1/submissions/status/{submission_id}")
        response.raise_for_status()
        return response.json()


class OpenMetadataAPI:
    def __init__(self) -> None:
        self.base_url = (
            env_str("OPENMETADATA_BASE_URL")
            or env_str("OPENMETADATA_SERVER_URL")
            or env_str("OPENMETADATA_URL")
            or require_env("OPENMETADATA_HOST")
        ).rstrip("/")
        if not self.base_url.endswith("/api"):
            self.api_base = f"{self.base_url}/api/v1"
        else:
            self.api_base = f"{self.base_url}/v1"
        self.service = env_str("OPENMETADATA_TRINO_SERVICE", "trino")
        self.database = env_str("OPENMETADATA_TRINO_DATABASE", "delta")
        self.schema = env_str("OPENMETADATA_TRINO_SCHEMA", "demo")
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        self._login()

    def _login(self) -> None:
        email = (
            env_str("OPENMETADATA_EMAIL")
            or env_str("OPENMETADATA_USERNAME")
            or env_str("OPENMETADATA_API_EMAIL")
            or "dataplatform.test@open-metadata.org"
        )
        password = (
            env_str("OPENMETADATA_PASSWORD")
            or env_str("OPENMETADATA_API_PASSWORD")
            or "Dataplatform@123"
        )
        encoded_password = base64.b64encode(password.encode("utf-8")).decode("ascii")
        response = self.session.post(
            f"{self.api_base}/users/login",
            json={"email": email, "password": encoded_password},
            timeout=30,
        )
        response.raise_for_status()
        token = response.json()["accessToken"]
        self.session.headers["Authorization"] = f"Bearer {token}"

    def _schema_fqn(self, schema_name: str | None = None) -> str:
        return f"{self.service}.{self.database}.{schema_name or self.schema}"

    def _table_fqn(self, table_name: str, schema_name: str | None = None) -> str:
        return f"{self._schema_fqn(schema_name)}.{table_name}"

    def _get_table(self, table_name: str, schema_name: str | None = None) -> dict[str, Any]:
        fqn = quote(self._table_fqn(table_name, schema_name), safe="")
        response = self.session.get(f"{self.api_base}/tables/name/{fqn}", timeout=30)
        response.raise_for_status()
        return response.json()

    def create_table(self, table_name: str, database: str, columns: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        payload = {
            "name": table_name,
            "databaseSchema": self._schema_fqn(database),
            "tableType": "Regular",
            "columns": columns or [{"name": "id", "dataType": "INT"}],
        }
        response = self.session.put(f"{self.api_base}/tables", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def update_table_columns(self, table_name: str, columns: list[dict[str, Any]], database: str | None = None) -> dict[str, Any]:
        payload = {
            "name": table_name,
            "databaseSchema": self._schema_fqn(database),
            "tableType": "Regular",
            "columns": columns,
        }
        response = self.session.put(f"{self.api_base}/tables", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def create_table_lineage(
        self,
        source_table: str,
        target_table: str,
        description: str | None = None,
        columns_lineage: list[dict[str, Any]] | None = None,
        source: str = "Manual",
        database: str | None = None,
    ) -> None:
        source_entity = self._get_table(source_table, database)
        target_entity = self._get_table(target_table, database)
        payload: dict[str, Any] = {
            "edge": {
                "fromEntity": {"id": source_entity["id"], "type": "table"},
                "toEntity": {"id": target_entity["id"], "type": "table"},
                "description": description,
                "lineageDetails": {
                    "source": source,
                    "columnsLineage": columns_lineage or [],
                },
            }
        }
        response = self.session.put(f"{self.api_base}/lineage", json=payload, timeout=30)
        response.raise_for_status()

