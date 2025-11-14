from sqlalchemy.orm import declarative_base, sessionmaker
from etude_core.config import settings

# --- Conditional Schema Logic ---
if settings.database.type == "mssql":
    DEFAULT_SCHEMA = "etude_core"

    class EtudeCoreBase:
        __table_args__ = {"schema": DEFAULT_SCHEMA}

    Base = declarative_base(cls=EtudeCoreBase)
else:
    DEFAULT_SCHEMA = None
    Base = declarative_base()

# --- Session Factory ---
SessionLocal = sessionmaker(autocommit=False, autoflush=False)
