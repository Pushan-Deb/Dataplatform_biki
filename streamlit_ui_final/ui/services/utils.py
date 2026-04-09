from datetime import datetime


def run_ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def with_run_ts(base_path: str):
    base = base_path.rstrip("/") + "/"
    return f"{base}run_ts={run_ts()}/"


def dataset_id_from_s3_path(s3_path: str) -> str:
    """Best-effort: s3://minio/<zone>/<domain>/<sub>/<dataset>/... -> <zone>.<domain>.<dataset>"""
    try:
        p = str(s3_path).replace("s3://minio/", "")
        parts = [x for x in p.split("/") if x]
        if len(parts) >= 3:
            zone = parts[0]
            domain = parts[1]
            dataset = parts[2]
            return f"{zone}.{domain}.{dataset}"
    except Exception:
        pass
    return ""
