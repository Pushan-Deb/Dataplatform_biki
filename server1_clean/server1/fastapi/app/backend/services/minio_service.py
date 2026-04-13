"""
services/minio_service.py - MinIO helpers (read parquet schema, upload).
"""
import io
import boto3
import pandas as pd
from botocore.client import Config
from backend.config import get_settings

settings = get_settings()


def _s3_client():
    # Always reload settings fresh and force 127.0.0.1
    s = get_settings()
    endpoint = s.AWS_ENDPOINT_URL.replace("localhost", "127.0.0.1")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=s.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=s.AWS_SECRET_ACCESS_KEY,
        region_name=s.AWS_DEFAULT_REGION,
        config=Config(signature_version="s3v4"),
    )


def ensure_buckets():
    """Create required MinIO buckets if they don't exist."""
    s3 = _s3_client()
    for bucket in [
        "business",
        settings.FEAST_DATA_BUCKET,
        settings.MLFLOW_ARTIFACT_BUCKET,
    ]:
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)
            print(f"[MinIO] Created bucket: {bucket}")


def list_parquet_files(bucket: str, prefix: str = "data/") -> list[str]:
    """List .parquet files under a prefix."""
    s3 = _s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [
        obj["Key"]
        for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]


def read_parquet_schema(bucket: str, key: str) -> dict:
    """Download a parquet file from MinIO and return its column schema."""
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    columns = [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
    return {"columns": columns, "row_count": len(df)}


def read_parquet_dataframe(bucket: str, key: str) -> pd.DataFrame:
    """Download and return a full parquet file as a DataFrame."""
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def upload_parquet(df: pd.DataFrame, bucket: str, key: str) -> str:
    """Upload a DataFrame as parquet to MinIO."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    s3 = _s3_client()
    # Ensure bucket exists before uploading
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        print(f"[MinIO] Created bucket: {bucket}")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return f"s3://{bucket}/{key}"


def upload_text(content: str, bucket: str, key: str) -> str:
    """Upload a text/yaml file to MinIO."""
    s3 = _s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def download_text(bucket: str, key: str) -> str:
    """Download a text file from MinIO."""
    s3 = _s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")
