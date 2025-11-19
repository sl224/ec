from sqlalchemy.orm import declarative_base, sessionmaker
from e2ude_core.config import settings
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.sqltypes import DateTime

E2UDE_DATETIME = DateTime().with_variant(DATETIME2(0), "mssql")

# Conditionally set a default schema for MSSQL based on CONFIG
if settings.database.type == "mssql":
    # Use the configured schema name (e.g., "e2ude_core_dev")
    DEFAULT_SCHEMA = settings.database.schema_name

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
    """
    if DEFAULT_SCHEMA:
        return f"{DEFAULT_SCHEMA}.{key}"
    return key


# A factory for creating new Session objects.
SessionLocal = sessionmaker(autocommit=False, autoflush=False)
