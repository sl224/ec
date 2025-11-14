from sqlalchemy.orm import declarative_base, sessionmaker

DEFAULT_SCHEMA = "etude_core"
DEFAULT_SCHEMA = ""


# 1. Define a class with the desired defaults (including schema)
class EtudeCoreBase:
    # This attribute will be inherited by all models
    __table_args__ = {"schema": DEFAULT_SCHEMA}


# 2. The Single Source of Truth for all Models
Base = declarative_base(cls=EtudeCoreBase)

# Session Factory (Optional but recommended to keep here)
# Can configure the engine later using Session.configure(bind=eng)
SessionLocal = sessionmaker(autocommit=False, autoflush=False)

# --- Model Exports ---
# Make all models available for import from etude_core.db.models
from .file import FileHashRegistry, FileMetadata
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
