"""
models_db.py - SQLAlchemy ORM for model definitions, run history and lineage.

Tables:
  model_definitions       - one row per model (upserted on each train)
  model_run_history       - one row per training run
  model_feature_lineage   - feature → model links
"""
from datetime import datetime
from sqlalchemy import (
    Column, String, Text, DateTime, Integer, Float, JSON, ForeignKey
)
from sqlalchemy.orm import relationship

from backend.models import Base, get_engine
from sqlalchemy.orm import sessionmaker


class ModelDefinition(Base):
    __tablename__ = "model_definitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    algorithm = Column(String(100), nullable=False)
    training_dataset = Column(Text, default="")
    feature_set = Column(JSON, default=list)          # list of feature names
    label_column = Column(String(255), default="label")
    stage = Column(String(50), default="Draft")       # Draft | Staging | Production | Archived
    hyperparameters = Column(JSON, default=dict)
    description = Column(Text, default="")
    owner = Column(String(255), default="ML Engineer")

    # Latest run metrics (denormalised for quick access)
    latest_auc = Column(Float, nullable=True)
    latest_accuracy = Column(Float, nullable=True)
    latest_run_id = Column(String(100), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    runs = relationship(
        "ModelRunHistory",
        back_populates="model",
        order_by="ModelRunHistory.started_at.desc()",
        cascade="all, delete-orphan",
    )


class ModelRunHistory(Base):
    __tablename__ = "model_run_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("model_definitions.id", ondelete="CASCADE"))
    model_name = Column(String(255), nullable=False, index=True)
    run_id = Column(String(100), unique=True, nullable=False)
    algorithm = Column(String(100))
    feature_set = Column(JSON, default=list)
    hyperparameters = Column(JSON, default=dict)
    status = Column(String(50), default="pending")    # pending | running | success | failed
    metrics = Column(JSON, default=dict)              # auc, accuracy, f1, etc.
    mlflow_run_id = Column(String(255), nullable=True)
    training_data_path = Column(Text, default="")
    model_artifact_path = Column(Text, default="")
    log = Column(Text, default="")
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)

    model = relationship("ModelDefinition", back_populates="runs")


class ModelFeatureLineage(Base):
    __tablename__ = "model_feature_lineage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_name = Column(String(255), nullable=False, index=True)
    feature_name = Column(String(255), nullable=False, index=True)
    run_id = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


def get_session_factory():
    engine = get_engine()
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_models_db():
    """Create model tables if they don't exist."""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    print("[DB] Model tables initialised.")
