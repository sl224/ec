from sqlalchemy.orm import declarative_base, sessionmaker
from etude_core.config import settings

# Conditionally set a default schema for MSSQL to keep tables organized.
if settings.database.type == "mssql":
    DEFAULT_SCHEMA = "etude_core"

    class EtudeCoreBase:
        __table_args__ = {"schema": DEFAULT_SCHEMA}

    Base = declarative_base(cls=EtudeCoreBase)
else:
    DEFAULT_SCHEMA = None
    Base = declarative_base()

# A factory for creating new Session objects.
SessionLocal = sessionmaker(autocommit=False, autoflush=False)
