from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

# Import Base AND the new schema_fkey helper
from etude_core.db.base_session import Base, schema_fkey


class FolderMetadata(Base):
    """
    Represents a root-level folder (or zip archive) being processed.
    """

    __tablename__ = "metadata_folder"
    id = Column("FolderID", Integer, primary_key=True)
    path = Column("FolderPath", String(500), nullable=False)
    files = relationship("FileMetadata", back_populates="folder")


class FileHashRegistry(Base):
    """
    Registry of unique file content hashes (MD5). This allows for content-based
    deduplication, where many file instances can point to one hash ID.
    """

    __tablename__ = "metadata_hash_registry"

    id = Column(Integer, primary_key=True)
    md5 = Column(String(32), unique=True, nullable=False, index=True)


class FileMetadata(Base):
    """
    Links a specific file instance (by its path within a folder) to its
    unique content hash in the `metadata_hash_registry`.
    """

    __tablename__ = "metadata_file"

    id = Column(Integer, primary_key=True)

    # Use `schema_fkey` to reference schema-qualified columns for foreign keys.
    folder_id = Column(
        Integer,
        ForeignKey(schema_fkey("metadata_folder.FolderID")),
        nullable=False,
        index=True,
    )

    # Use `schema_fkey` for the hash registry foreign key.
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
