from enum import Enum as PyEnum
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Enum, func, Index
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mssql import DATETIME2

from .base import Base

# Create a single Base for all models to share


# --- 1. The Source (Folders) ---
class FolderMetadata(Base):
    """
    Represents a root-level folder (or zip) to be processed.
    Maps legacy column names (FolderID) to Pythonic attributes (id).
    """

    __tablename__ = "folder_metadata_config"
    id = Column("FolderID", Integer, primary_key=True)
    path = Column("FolderPath", String(500), nullable=False)
    files = relationship("FileMetadata", back_populates="folder")


# --- 2. The Content (Hashes) ---


class FileHashRegistry(Base):
    """
    Registry of unique file content.
    Used for deduplication: many files can point to one Hash ID.
    """

    __tablename__ = "file_hash_registry"

    id = Column(Integer, primary_key=True)
    md5 = Column(String(32), unique=True, nullable=False, index=True)


# --- 3. The Instance (Files) ---


class FileMetadata(Base):
    """
    The 'Rosetta Stone'.
    Links a specific file instance (in a folder) to its content hash.
    """

    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True)

    # Link back to the source folder
    folder_id = Column(
        Integer,
        ForeignKey("folder_metadata_config.FolderID"),
        nullable=False,
        index=True,
    )

    # Link to the unique content hash
    hash_id = Column(
        Integer, ForeignKey("file_hash_registry.id"), nullable=False, index=True
    )

    relative_path = Column(String(500), nullable=False)
    file_type = Column(String(50), index=True)
    file_size_bytes = Column(Integer)

    # Relationships
    folder = relationship("FolderMetadata", back_populates="files")
    hash_info = relationship("FileHashRegistry")


# --- Enums ---
class StatusEnum(PyEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"


class ProcessingSession(Base):
    """
    Represents a single execution run (a "session") of the ETL process
    against a specific FolderID.
    """

    __tablename__ = "processing_sessions"
    id = Column(Integer, primary_key=True)

    # The FolderID (from your external table) this session is processing
    folder_id = Column(Integer, nullable=False, index=True)
    git_hash = Column(String(40), nullable=True, index=True)
    user_name = Column(String(40), nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.RUNNING)
    start_time = Column(DateTime, server_default=func.now())
    end_time = Column(DateTime, nullable=True)

    # Python-only link to all child jobs
    jobs = relationship("ProcessingJob", back_populates="session", lazy="dynamic")


class ProcessingJob(Base):
    """
    Represents the processing of a single file for a single pipeline.
    (e.g., running "VersionsSummaryHandler" on "abc_Versions.xml")
    """

    __tablename__ = "processing_jobs"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("processing_sessions.id"), nullable=False)

    job_name = Column(String(500))  # e.g., "VersionsSummaryHandler: abc_Versions.xml"
    file_type = Column(String(50), index=True)  # Keep this for context
    pipeline_id = Column(String(255), nullable=True, index=True)
    rows_uploaded = Column(Integer, nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.PENDING, index=True)
    message = Column(String, nullable=True)
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)

    # --- NEW COLUMNS ---
    # Link to the FileMetadata table
    file_id = Column(Integer, ForeignKey("file_metadata.id"), nullable=True, index=True)
    # Link to the FileHashRegistry table (for skip logic)
    hash_id = Column(
        Integer, ForeignKey("file_hash_registry.id"), nullable=True, index=True
    )

    # Python-only link back to the parent session
    session = relationship("ProcessingSession", back_populates="jobs")

    __table_args__ = (
        Index("ix_folder_status_type", "file_type", "status"),
        # Index to find a specific job in a session
        Index("ix_job_lookup", "session_id", "pipeline_id", "file_id", unique=True),
        # Index to check for completed hashes across *all* sessions
        Index("ix_hash_skip_lookup", "pipeline_id", "hash_id", "status"),
    )


# --- 4. The Data (Leaf Tables) ---


class TmptrData(Base):
    """
    Example Leaf Table.
    Keyed by HashID (Content), NOT FileID (Instance).
    """

    __tablename__ = "tmptr"

    # Primary Key is the HashID + Line Number
    hash_id = Column(Integer, ForeignKey("file_hash_registry.id"), primary_key=True)
    line_number = Column(Integer, primary_key=True)

    datetime = Column(DateTime().with_variant(DATETIME2(3), "mssql"))
    category = Column(String)
    temp_f = Column(Integer)
    temp_c = Column(Integer)
