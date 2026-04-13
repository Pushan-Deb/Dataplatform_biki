from __future__ import annotations

import os
from pathlib import Path


_LOADED = False


def _candidate_env_files() -> list[Path]:
    here = Path(__file__).resolve().parent
    return [
        here / "platform.env",
        here / ".env",
        Path("/opt/airflow/config/platform.env"),
        Path("/opt/airflow/config/.env"),
        Path("/opt/airflow/.env"),
    ]


def load_platform_env() -> None:
    global _LOADED
    if _LOADED:
        return
    for path in _candidate_env_files():
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
    _LOADED = True


def env_str(name: str, default: str | None = None) -> str | None:
    load_platform_env()
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def env_int(name: str, default: int | None = None) -> int | None:
    value = env_str(name)
    if value is None:
        return default
    return int(value)


def require_env(name: str) -> str:
    value = env_str(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
