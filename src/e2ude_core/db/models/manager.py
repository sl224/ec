from enum import Enum as PyEnum

from sqlalchemy import (
    VARBINARY,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import relationship

from e2ude_core.db.base_session import Base, DEFAULT_SCHEMA, E2UDE_DATETIME, schema_fkey


class StatusEnum(PyEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


class ArtifactManifest(Base):
    """Registry of valid parser output currently materialized by content hash."""

    __tablename__ = "metadata_artifact_manifest"

    content_hash = Column(VARBINARY(16), primary_key=True, nullable=False)
    artifact_key = Column(String(100), primary_key=True, nullable=False)
    target_table = Column(String(100), nullable=False)
    parser_version = Column(Integer, nullable=False, default=1)
    row_count = Column(Integer, nullable=False, default=0, server_default="0")
    created_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_artifact_key", "artifact_key"),
        Index("ix_artifact_target_table", "target_table"),
        {"schema": DEFAULT_SCHEMA},
    )


class ProcessingSession(Base):
    """One run of the CLI or pipeline."""

    __tablename__ = "processing_sessions"

    id = Column(Integer, primary_key=True)
    git_hash = Column(String(40), nullable=True, index=True)
    user_name = Column(String(40), nullable=True)
    host_name = Column(String(40), nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.PENDING, index=True)
    start_time = Column(E2UDE_DATETIME(), server_default=func.now())
    end_time = Column(E2UDE_DATETIME(), nullable=True)

    jobs = relationship("ProcessingJob", back_populates="session", lazy="dynamic")


class ProcessingJob(Base):
    """Attempt history for catalog and parser work inside a run."""

    __tablename__ = "processing_jobs"

    session_id = Column(
        Integer, ForeignKey(schema_fkey("processing_sessions.id")), nullable=False
    )
    id = Column(Integer, primary_key=True)

    archive_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_archive.id")),
        nullable=True,
        index=True,
    )
    file_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_file.id")),
        nullable=True,
        index=True,
    )
    content_hash = Column(VARBINARY(16), nullable=True, index=True)
    file_type = Column(String(50), nullable=True, index=True)
    parser_id = Column(String(255), nullable=True, index=True)
    target_table = Column(String(100), nullable=True, index=True)
    parser_version = Column(Integer, default=1, nullable=False)
    rows_uploaded = Column(Integer, nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.PENDING, index=True)
    message = Column(String, nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)

    session = relationship("ProcessingSession", back_populates="jobs")

    __table_args__ = (
        Index(
            "ix_job_lookup",
            "session_id",
            "parser_id",
            "content_hash",
            "target_table",
        ),
        {"schema": DEFAULT_SCHEMA},
    )
