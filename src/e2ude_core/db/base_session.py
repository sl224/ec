from sqlalchemy.orm import declarative_base, sessionmaker
from e2ude_core.config import settings

# Conditionally set a default schema for MSSQL to keep tables organized.
if settings.database.type == "mssql":
    DEFAULT_SCHEMA = "e2ude_core"

    class e2udeCoreBase:
        __table_args__ = {"schema": DEFAULT_SCHEMA}

    Base = declarative_base(cls=e2udeCoreBase)
else:
    DEFAULT_SCHEMA = None
    Base = declarative_base()


def schema_fkey(key: str) -> str:
    """
    Returns a schema-qualified foreign key string if a schema is defined,
    otherwise returns the simple key.

    - MSSQL: "e2ude_core.table.column"
    - SQLite: "table.column"
    """
    if DEFAULT_SCHEMA:
        return f"{DEFAULT_SCHEMA}.{key}"
    return key


# A factory for creating new Session objects.
SessionLocal = sessionmaker(autocommit=False, autoflush=False)
