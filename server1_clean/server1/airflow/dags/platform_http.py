from __future__ import annotations

import base64
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from platform_env import env_int, env_str


def _service_keys(service_name: str) -> dict[str, str]:
    prefix = service_name.upper().replace("-", "_")
    return {
        "auth_type": f"{prefix}_AUTH_TYPE",
        "username": f"{prefix}_USERNAME",
        "password": f"{prefix}_PASSWORD",
        "token": f"{prefix}_TOKEN",
        "timeout": f"{prefix}_TIMEOUT",
        "verify_ssl": f"{prefix}_VERIFY_SSL",
    }


def _verify_ssl(value: str | None) -> bool:
    if value is None:
        return env_str("REQUESTS_VERIFY_SSL", "true").lower() not in {"0", "false", "no"}
    return value.lower() not in {"0", "false", "no"}


def build_session(service_name: str, extra_headers: dict[str, str] | None = None) -> requests.Session:
    keys = _service_keys(service_name)
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        allowed_methods={"GET", "POST", "PUT", "PATCH", "DELETE"},
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.verify = _verify_ssl(env_str(keys["verify_ssl"]))
    session.headers.update(extra_headers or {})
    session.headers.setdefault("Accept", "application/json")

    auth_type = (env_str(keys["auth_type"]) or "").strip().lower()
    username = env_str(keys["username"])
    password = env_str(keys["password"])
    token = env_str(keys["token"])

    if auth_type == "bearer" and token:
        session.headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "basic" and username is not None and password is not None:
        encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        session.headers["Authorization"] = f"Basic {encoded}"
    elif token:
        session.headers["Authorization"] = f"Bearer {token}"
    elif username is not None and password is not None:
        session.auth = (username, password)

    timeout = env_int(keys["timeout"], 30)
    session.request = _wrap_request(session.request, timeout)
    return session


def _wrap_request(func: Any, default_timeout: int):
    def wrapped(method: str, url: str, **kwargs: Any):
        kwargs.setdefault("timeout", default_timeout)
        return func(method, url, **kwargs)

    return wrapped
