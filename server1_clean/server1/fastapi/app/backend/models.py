"""
models.py – SQLAlchemy ORM models for feature definitions & history.
"""
from datetime import datetime
from sqlalchemy import (
    Column, String, Text, DateTime, Integer, JSON, Boolean, ForeignKey, create_engine
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from backend.config import get_settings

Base = declarative_base()
settings = get_settings()


class FeatureDefinition(Base):
    __tablename__ = "feature_definitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    entity = Column(String(255), nullable=False)
    feature_sql = Column(Text, nullable=False)
    description = Column(Text, default="")
    window = Column(String(50), default="90d")
    refresh_cadence = Column(String(50), default="Daily")
    owner = Column(String(255), default="ML Engineer")
    source_datasets = Column(JSON, default=list)   # list of dataset names
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    feast_applied = Column(Boolean, default=False)
    feast_apply_log = Column(Text, default="")

    history = relationship(
        "FeatureDefinitionHistory",
        back_populates="definition",
        order_by="FeatureDefinitionHistory.version.desc()",
        cascade="all, delete-orphan",
    )


class FeatureDefinitionHistory(Base):
    __tablename__ = "feature_definition_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    definition_id = Column(Integer, ForeignKey("feature_definitions.id", ondelete="CASCADE"))
    version = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False)
    entity = Column(String(255))
    feature_sql = Column(Text)
    description = Column(Text)
    window = Column(String(50))
    refresh_cadence = Column(String(50))
    owner = Column(String(255))
    source_datasets = Column(JSON, default=list)
    snapshot_at = Column(DateTime, default=datetime.utcnow)
    change_note = Column(Text, default="")

    definition = relationship("FeatureDefinition", back_populates="history")


class MaterializationJob(Base):
    __tablename__ = "materialization_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    feature_name = Column(String(255), nullable=False, index=True)
    status = Column(String(50), default="pending")   # pending | running | success | failed
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    log = Column(Text, default="")
    offline_path = Column(Text, default="")
    online_path = Column(Text, default="")


def get_engine():
    return create_engine(settings.DATABASE_URL, pool_pre_ping=True)


def get_session_factory():
    engine = get_engine()
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db():
    """Create all tables if they don't exist."""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    print("[DB] Tables initialised.")
