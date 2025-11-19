from sqlalchemy.orm import declarative_base, sessionmaker
from e2ude_core.config import settings
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.sqltypes import DateTime, TypeDecorator


def E2UDE_DATETIME(precision: int | None = None) -> TypeDecorator:
    """
    Factory function that returns a database-specific DATETIME type.

    - For MSSQL, returns DATETIME2 with specified precision.
    - For other databases (like SQLite), returns a standard DateTime.
    """
    if settings.database.type == "mssql":
        return DATETIME2(precision=precision)
    else:
        # SQLite's native DATETIME does not support precision.
        return DateTime(timezone=False)


# Conditionally set a default schema for MSSQL to keep tables organized.
if settings.database.type == "mssql":
    DEFAULT_SCHEMA = "e2ude_core_dev"

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
