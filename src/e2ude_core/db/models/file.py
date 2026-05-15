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
    """Discovered archive locator plus filename-derived labels."""

    __tablename__ = "metadata_archive"

    id = Column(Integer, primary_key=True)
    archive_key = Column(String(128), nullable=False)
    buno = Column(String(6), nullable=False)
    archive_datetime = Column(E2UDE_DATETIME(0), nullable=False)
    locator_key = Column(String(500), nullable=False)
    locator_path = Column(String(500), nullable=False)
    locator_size_bytes = Column(BigInteger, nullable=False)
    locator_mtime_ns = Column(BigInteger, nullable=False)
    first_seen_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())
    last_seen_at = Column(E2UDE_DATETIME(), nullable=False, server_default=func.now())
    is_present = Column(Boolean, nullable=False, default=True, server_default="1")
    cataloged_at = Column(E2UDE_DATETIME(), nullable=True)
    catalog_version = Column(Integer, nullable=False, default=0, server_default="0")
    catalog_signature = Column(String(40), nullable=True)

    files = relationship("FileMetadata", back_populates="archive")

    __table_args__ = (
        Index("ix_archive_key", "archive_key"),
        Index("ix_archive_buno_datetime", "buno", "archive_datetime"),
        Index("ix_unique_archive_locator_key", "locator_key", unique=True),
        Index("ix_archive_locator_path", "locator_path"),
        {"schema": DEFAULT_SCHEMA},
    )


class FileMetadata(Base):
    """One file member inside an archive, optionally linked to a content hash."""

    __tablename__ = "metadata_file"

    id = Column(Integer, primary_key=True)
    archive_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_archive.id")),
        nullable=False,
        index=True,
    )
    content_hash = Column(VARBINARY(16), nullable=True, index=True)
    relative_path = Column(String(500), nullable=False)
    file_size_bytes = Column(Integer)
    compressed_size_bytes = Column(Integer)
    crc32 = Column(BigInteger)
    zip_depth = Column(Integer, nullable=False, default=0, server_default="0")

    archive = relationship("ArchiveMetadata", back_populates="files")

    __table_args__ = (
        Index("ix_file_archive_path", "archive_id", "relative_path", unique=True),
        {"schema": DEFAULT_SCHEMA},
    )
