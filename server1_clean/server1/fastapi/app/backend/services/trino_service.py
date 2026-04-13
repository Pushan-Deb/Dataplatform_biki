"""
services/trino_service.py - lightweight SQL source detection helpers.
"""
import re


def detect_source_tables_from_sql(sql: str) -> list[str]:
    pattern = re.compile(
        r"\b(?:from|join)\s+([a-zA-Z0-9_.\"`]+)",
        flags=re.IGNORECASE,
    )
    tables = []
    for match in pattern.findall(sql or ""):
        table = match.strip().strip('"`')
        if table not in tables:
            tables.append(table)
    return tables


def validate_sql_via_trino(sql: str) -> dict:
    if not sql or not sql.strip():
        return {"valid": False, "error": "SQL is empty.", "plan_summary": ""}
    return {
        "valid": True,
        "error": None,
        "plan_summary": "Static validation only. Trino execution is not configured in this stack.",
    }

