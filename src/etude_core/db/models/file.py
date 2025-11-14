from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from etude_core.db.base_session import Base


class FolderMetadata(Base):
    """
    Represents a root-level folder (or zip archive) being processed.
    """

    __tablename__ = "folder_metadata"
    id = Column("FolderID", Integer, primary_key=True)
    path = Column("FolderPath", String(500), nullable=False)
    files = relationship("FileMetadata", back_populates="folder")


class FileHashRegistry(Base):
    """
    Registry of unique file content hashes (MD5). This allows for content-based
    deduplication, where many file instances can point to one hash ID.
    """

    __tablename__ = "file_hash_registry"

    id = Column(Integer, primary_key=True)
    md5 = Column(String(32), unique=True, nullable=False, index=True)


class FileMetadata(Base):
    """
    Links a specific file instance (by its path within a folder) to its
    unique content hash in the `file_hash_registry`.
    """

    __tablename__ = "file_metadata"

    id = Column(Integer, primary_key=True)

    folder_id = Column(
        Integer,
        ForeignKey("folder_metadata.FolderID"),
        nullable=False,
        index=True,
    )

    hash_id = Column(
        Integer,
        ForeignKey("file_hash_registry.id"),
        nullable=False,
        index=True,
    )

    relative_path = Column(String(500), nullable=False)
    file_type = Column(String(50), index=True)
    file_size_bytes = Column(Integer)

    # Relationships
    folder = relationship("FolderMetadata", back_populates="files")
    hash_info = relationship("FileHashRegistry")
