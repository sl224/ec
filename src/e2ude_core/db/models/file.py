from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    VARBINARY,
    func,
)
from sqlalchemy.orm import relationship

from e2ude_core.db.base_session import Base, DEFAULT_SCHEMA, E2UDE_DATETIME, schema_fkey


class ArchiveMetadata(Base):
    """Source archive inventory plus metadata scan freshness."""

    __tablename__ = "metadata_archive"

    id = Column(Integer, primary_key=True)
    buno = Column(String(6), nullable=False)
    archive_datetime = Column(E2UDE_DATETIME(0), nullable=False)
    source_path = Column(String(500), unique=True, nullable=False)
    source_size_bytes = Column(BigInteger, nullable=False)
    source_mtime_ns = Column(BigInteger, nullable=False)
    first_seen_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())
    last_seen_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())
    is_present = Column(Boolean, nullable=False, default=True, server_default="1")
    required_scan_version = Column(Integer, nullable=False, default=1)
    completed_scan_version = Column(Integer, nullable=False, default=0)

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
    """Registry of unique file content hashes."""

    __tablename__ = "metadata_hash_registry"

    id = Column(Integer, primary_key=True)
    md5 = Column(VARBINARY(16), unique=True, nullable=False, index=True)


class FileMetadata(Base):
    """Links one archived file instance to its content hash."""

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

    archive = relationship("ArchiveMetadata", back_populates="files")
    hash_info = relationship("FileHashRegistry")
