from sqlalchemy.orm import declarative_base, sessionmaker
# --- NEW IMPORT ---
from etude_core.config import settings

# --- FIX: Conditional Schema Logic ---
# Check the database type from your settings
# This assumes your settings.database.type is 'mssql' for MSSQL
# and 'sqlite3' (or similar) for SQLite.
if settings.database.type == "mssql":
    DEFAULT_SCHEMA = "etude_core"
else:
    # For SQLite, the schema must be None.
    # An empty string "" is what caused the invalid ".table.column" paths.
    DEFAULT_SCHEMA = None
# --- END FIX ---


# 1. Define a class with the desired defaults (including schema)
class EtudeCoreBase:
    # This attribute will be inherited by all models
    # It will now be {"schema": "etude_core"} for MSSQL
    # and {"schema": None} for SQLite, which is correct for both.
    __table_args__ = {"schema": DEFAULT_SCHEMA}


# 2. The Single Source of Truth for all Models
Base = declarative_base(cls=EtudeCoreBase)

# Session Factory (Optional but recommended to keep here)
# Can configure the engine later using Session.configure(bind=eng)
SessionLocal = sessionmaker(autocommit=False, autoflush=False)

# --- Model Exports ---
# Make all models available for import from etude_core.db.models
# (Added FolderMetadata to the list)
from .file import FileHashRegistry, FileMetadata, FolderMetadata
from .manager import ProcessingJob, ProcessingSession, StatusEnum

# Example derived tables
from .rsm_tmptr import TmptrData
from .rsm_mcdata import (
    Rpcs,
    RpcsPres,
    NavData,
    RadarState,
    RotoScan,
    GfcDb,
    PfcDb,
    RfcDb,
    LcsTemp,
    McInDiscr,
)


# Explicitly declare the public API of the 'models' package
__all__ = [
    "Base",
    "SessionLocal",
    "StatusEnum",
    "FolderMetadata", # <-- Added
    "FileHashRegistry",
    "FileMetadata",
    "ProcessingSession",
    "ProcessingJob",
    "TmptrData",
    "Rpcs",
    "RpcsPres",
    "NavData",
    "RadarState",
    "RotoScan",
    "GfcDb",
    "PfcDb",
    "RfcDb",
    "LcsTemp",
    "McInDiscr",
]