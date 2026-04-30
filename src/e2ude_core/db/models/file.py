from enum import Enum as PyEnum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    VARBINARY,
    func,
)
from sqlalchemy.orm import relationship

from e2ude_core.db.base_session import Base, schema_fkey, DEFAULT_SCHEMA, E2UDE_DATETIME


class ArchiveStateEnum(PyEnum):
    NEEDS_SCAN = "NEEDS_SCAN"
    NEEDS_PROCESSING = "NEEDS_PROCESSING"
    UP_TO_DATE = "UP_TO_DATE"


class ArchiveMetadata(Base):
    """
    Canonical inventory and hot-path work state for one source archive.
    """

    __tablename__ = "metadata_archive"
    id = Column("id", Integer, primary_key=True)
    buno = Column("buno", String(6), nullable=False)
    archive_datetime = Column("archive_datetime", E2UDE_DATETIME(0), nullable=False)
    source_path = Column("source_path", String(500), unique=True, nullable=False)
    source_size_bytes = Column(BigInteger, nullable=False)
    source_mtime_ns = Column(BigInteger, nullable=False)
    first_seen_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())
    last_seen_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())
    is_present = Column(Boolean, nullable=False, default=True, server_default="1")

    required_scan_version = Column(Integer, nullable=False, default=1)
    completed_scan_version = Column(Integer, nullable=False, default=0)
    required_handler_generation = Column(String(40), nullable=False)
    completed_handler_generation = Column(String(40), nullable=True)
    state = Column(
        Enum(ArchiveStateEnum),
        nullable=False,
        default=ArchiveStateEnum.NEEDS_SCAN,
        index=True,
    )
    work_reason = Column(String(255), nullable=True)
    last_success_at = Column(E2UDE_DATETIME(), nullable=True)
    last_error_at = Column(E2UDE_DATETIME(), nullable=True)
    last_error_message = Column(String, nullable=True)

    files = relationship("FileMetadata", back_populates="archive")

    __table_args__ = (
        Index("ix_unique_archive", "buno", "archive_datetime"),
        {"schema": DEFAULT_SCHEMA},
    )


class DiscoveryDirectoryMetadata(Base):
    """Directory-level discovery snapshots for source membership tracking."""

    __tablename__ = "metadata_discovery_directory"

    path = Column(String(500), primary_key=True)
    mtime_ns = Column(BigInteger, nullable=False)
    contains_archives = Column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    last_checked_at = Column(
        E2UDE_DATETIME(),
        nullable=False,
        server_default=func.now(),
    )
    last_scanned_at = Column(E2UDE_DATETIME(), nullable=True)

    __table_args__ = ({"schema": DEFAULT_SCHEMA},)


class FileHashRegistry(Base):
    """
    Registry of unique file content hashes (MD5).
    """

    __tablename__ = "metadata_hash_registry"

    id = Column(Integer, primary_key=True)
    md5 = Column(VARBINARY(16), unique=True, nullable=False, index=True)


class FileMetadata(Base):
    """
    Links a specific file instance to its unique content hash.
    """

    __tablename__ = "metadata_file"

    id = Column(Integer, primary_key=True)

    archive_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_archive.id")),
        nullable=False,
        index=True,
    )

    hash_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_hash_registry.id")),
        nullable=False,
        index=True,
    )

    relative_path = Column(String(500), nullable=False)
    file_type = Column(String(50), index=True)
    file_size_bytes = Column(Integer)

    # Relationships
    archive = relationship("ArchiveMetadata", back_populates="files")
    hash_info = relationship("FileHashRegistry")
