from enum import Enum as PyEnum
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Enum, func, Index
from sqlalchemy.orm import relationship

from e2ude_core.db.base_session import Base, schema_fkey, DEFAULT_SCHEMA, E2UDE_DATETIME


class StatusEnum(PyEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


class ArtifactManifest(Base):
    """
    A lightweight registry of valid data currently stored in the database.
    """

    __tablename__ = "metadata_artifact_manifest"

    hash_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_hash_registry.id")),
        primary_key=True,
        nullable=False,
    )
    target_table = Column(String(100), primary_key=True, nullable=False)
    handler_version = Column(Integer, nullable=False, default=1)

    __table_args__ = (
        # Clustered index (implied by PK) is usually sufficient,
        # but explicit index ensures covering behavior if needed.
        Index("ix_artifact_lookup", "hash_id", "target_table"),
        {"schema": DEFAULT_SCHEMA},
    )


class ProcessingSession(Base):
    __tablename__ = "processing_sessions"
    id = Column(Integer, primary_key=True)

    folder_id = Column(Integer, nullable=False, index=True)
    git_hash = Column(String(40), nullable=True, index=True)
    user_name = Column(String(40), nullable=True)
    host_name = Column(String(40), nullable=True)
    status = Column(Enum(StatusEnum), default="UNINITIALIZED")
    start_time = Column(E2UDE_DATETIME(), server_default=func.now())
    end_time = Column(E2UDE_DATETIME(), nullable=True)
    jobs = relationship("ProcessingJob", back_populates="session", lazy="dynamic")


class ProcessingJob(Base):
    """
    Audit Log for ETL execution.
    No longer used for logic/skipping checks.
    """

    __tablename__ = "processing_jobs"
    session_id = Column(
        Integer, ForeignKey(schema_fkey("processing_sessions.id")), nullable=False
    )
    id = Column(Integer, primary_key=True)

    job_name = Column(String(500))
    file_type = Column(String(50), index=True)
    pipeline_id = Column(String(255), nullable=True)  # Index removed (not queried)
    target_name = Column(String(50), nullable=True)  # Index removed
    handler_version = Column(Integer, default=1, nullable=False)
    rows_uploaded = Column(Integer, nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.PENDING, index=True)
    message = Column(String, nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    dataset_key = Column(String(50), nullable=True)

    file_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_file.id")),
        nullable=True,
        index=True,
    )

    hash_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_hash_registry.id")),
        nullable=True,
        index=True,
    )

    session = relationship("ProcessingSession", back_populates="jobs")

    __table_args__ = (
        # Keep index for "Find jobs in this session"
        Index(
            "ix_job_lookup",
            "session_id",
            "pipeline_id",
            "file_id",
            "dataset_key",
            unique=True,
        ),
        # DELETED: "ix_hash_skip_lookup" - Replaced by ArtifactManifest
        {"schema": DEFAULT_SCHEMA},
    )
