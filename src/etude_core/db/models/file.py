from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from etude_core.db.base_session import Base


# --- 1. The Source (Folders) ---
class FolderMetadata(Base):
    """
    Represents a root-level folder (or zip) to be processed.
    Maps legacy column names (FolderID) to Pythonic attributes (id).
    """

    __tablename__ = "folder_metadata"
    id = Column("FolderID", Integer, primary_key=True)
    path = Column("FolderPath", String(500), nullable=False)
    files = relationship("FileMetadata", back_populates="folder")


class FileHashRegistry(Base):
    """
    Registry of unique file content.
    Used for deduplication: many files can point to one Hash ID.
    """

    __tablename__ = "file_hash_registry"

    id = Column(Integer, primary_key=True)
    md5 = Column(String(32), unique=True, nullable=False, index=True)


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
        ForeignKey("folder_metadata.FolderID"),
        nullable=False,
        index=True,
    )

    # Link to the unique content hash
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
