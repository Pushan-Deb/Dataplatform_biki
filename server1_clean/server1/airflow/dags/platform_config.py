from __future__ import annotations

from platform_env import env_str, require_env


def get_openmetadata_service() -> str:
    return env_str("OPENMETADATA_TRINO_SERVICE", "trino")


def get_openmetadata_database() -> str:
    return env_str("OPENMETADATA_TRINO_DATABASE", "delta")


def get_openmetadata_schema() -> str:
    return env_str("OPENMETADATA_TRINO_SCHEMA", "demo")


def get_required(name: str) -> str:
    return require_env(name)
