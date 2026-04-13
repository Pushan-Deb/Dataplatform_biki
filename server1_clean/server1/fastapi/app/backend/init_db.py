"""
init_db.py – Standalone script to create all PostgreSQL tables and Feast registry DB.

Run once before starting the API:
    python init_db.py
"""
import sqlalchemy
from sqlalchemy import text
from backend.models import init_db, get_engine
from backend.config import get_settings

settings = get_settings()


def create_feast_registry_db():
    """Create the feast_registry database if it doesn't exist."""
    # Connect to the main postgres DB to create registry DB
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
        f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}",
        isolation_level="AUTOCOMMIT",
    )
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname = '{settings.FEAST_REGISTRY_DB}'")
        )
        if not result.fetchone():
            conn.execute(text(f"CREATE DATABASE {settings.FEAST_REGISTRY_DB}"))
            print(f"[init_db] Created database: {settings.FEAST_REGISTRY_DB}")
        else:
            print(f"[init_db] Database already exists: {settings.FEAST_REGISTRY_DB}")


def create_feast_online_schema():
    """Create feast_online schema in the main feature_db."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS feast_online"))
        conn.commit()
        print("[init_db] Schema 'feast_online' ready.")


def init_model_tables():
    """Create model-related tables."""
    from models_db import init_models_db
    init_models_db()


if __name__ == "__main__":
    print("=== Feature Platform DB Initialisation ===")

    print("\n1. Creating application tables in feature_db...")
    init_db()

    print("\n2. Creating feast_registry database...")
    try:
        create_feast_registry_db()
    except Exception as e:
        print(f"   Warning: {e}")

    print("\n3. Creating feast_online schema...")
    try:
        create_feast_online_schema()
    except Exception as e:
        print(f"   Warning: {e}")

    print("\n4. Creating model tables...")
    try:
        init_model_tables()
    except Exception as e:
        print(f"   Warning: {e}")

    print("\n✅ Database initialisation complete.")
