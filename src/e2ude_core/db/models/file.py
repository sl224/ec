from sqlalchemy import Column, Integer, String, ForeignKey, Index, VARBINARY
from sqlalchemy.orm import relationship

# Import Base AND the new schema_fkey helper
from e2ude_core.db.base_session import Base, schema_fkey, DEFAULT_SCHEMA, E2UDE_DATETIME


class FolderMetadata(Base):
    """
    Represents a root-level folder (or zip archive) being processed.
    """

    __tablename__ = "metadata_folder"
    id = Column("id", Integer, primary_key=True)
    buno = Column("buno", String(6), nullable=False)
    folder_datetime = Column("folder_datetime", E2UDE_DATETIME(0), nullable=False)
    path = Column("path", String(500), unique=True, nullable=False)
    
    # REMOVED: scan_version. 
    
    files = relationship("FileMetadata", back_populates="folder")

    __table_args__ = (
        Index("ix_unique_zip", "buno", "folder_datetime"),
        {"schema": DEFAULT_SCHEMA},
    )


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

    folder_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_folder.id")),
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
    folder = relationship("FolderMetadata", back_populates="files")
    hash_info = relationship("FileHashRegistry")