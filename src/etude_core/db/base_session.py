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


def schema_fkey(key: str) -> str:
    """
    Returns a schema-qualified foreign key string if a schema is defined,
    otherwise returns the simple key.

    - MSSQL: "etude_core.table.column"
    - SQLite: "table.column"
    """
    if DEFAULT_SCHEMA:
        return f"{DEFAULT_SCHEMA}.{key}"
    return key


# A factory for creating new Session objects.
SessionLocal = sessionmaker(autocommit=False, autoflush=False)
