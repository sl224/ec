from enum import Enum as PyEnum
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Enum, func, Index
from sqlalchemy.orm import relationship
from etude_core.db.base_session import Base


class StatusEnum(PyEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


class ProcessingSession(Base):
    """
    Represents a single execution run of the ETL against a specific folder.
    """

    __tablename__ = "processing_sessions"
    id = Column(Integer, primary_key=True)

    folder_id = Column(Integer, nullable=False, index=True)
    git_hash = Column(String(40), nullable=True, index=True)
    user_name = Column(String(40), nullable=True)
    status = Column(Enum(StatusEnum), default="UNINITIALIZED")
    start_time = Column(DateTime, server_default=func.now())
    end_time = Column(DateTime, nullable=True)
    jobs = relationship("ProcessingJob", back_populates="session", lazy="dynamic")


class ProcessingJob(Base):
    """
    Represents a single unit of work, e.g., processing one table from one file.
    """

    __tablename__ = "processing_jobs"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("processing_sessions.id"), nullable=False)

    job_name = Column(String(500))  # e.g., "VersionsSummaryHandler: abc_Versions.xml"
    file_type = Column(String(50), index=True)
    pipeline_id = Column(String(255), nullable=True, index=True)
    rows_uploaded = Column(Integer, nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.PENDING, index=True)
    message = Column(String, nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    dataset_key = Column(String(50), nullable=True, index=True)

    file_id = Column(
        Integer,
        ForeignKey("file_metadata.id"),
        nullable=True,
        index=True,
    )
    hash_id = Column(
        Integer,
        ForeignKey("file_hash_registry.id"),
        nullable=True,
        index=True,
    )

    session = relationship("ProcessingSession", back_populates="jobs")

    __table_args__ = (
        Index("ix_folder_status_type", "file_type", "status"),
        # Unique index to find a specific job within a session.
        Index(
            "ix_job_lookup",
            "session_id",
            "pipeline_id",
            "file_id",
            "dataset_key",
            unique=True,
        ),
        # Index to check for completed hashes across *all* sessions
        Index("ix_hash_skip_lookup", "pipeline_id", "hash_id", "dataset_key", "status"),
    )
