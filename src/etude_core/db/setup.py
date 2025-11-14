import logging
import sqlalchemy as sa
from sqlalchemy.schema import CreateSchema

from etude_core.config import settings
from etude_core.db.base_session import Base

# Import models to populate `Base.metadata` with table definitions
import etude_core.db.models  # noqa: F401
from etude_core.db.models import FolderMetadata

logger = logging.getLogger(__name__)


def initialize_database(eng: sa.Engine, reset_tables: bool = False):
    """
    Ensures the necessary database schema exists (for MSSQL)
    and optionally resets all tables.
    """
    # 1. Schema Creation (MSSQL-specific)
    if settings.database.type == "mssql":
        from etude_core.db.base_session import DEFAULT_SCHEMA

        if not DEFAULT_SCHEMA:
            logger.error("MSSQL is selected but DEFAULT_SCHEMA is not set. Exiting.")
            exit(1)

        logger.info(f"Ensuring MSSQL schema '{DEFAULT_SCHEMA}' exists...")
        with eng.connect() as conn:
            if not conn.dialect.has_schema(conn, DEFAULT_SCHEMA):
                conn.execute(CreateSchema(DEFAULT_SCHEMA))
                logger.info(f"Schema '{DEFAULT_SCHEMA}' created.")
            conn.commit()

    # 2. Table Creation / Reset
    if reset_tables:
        logger.info("Resetting and creating database tables...")
        # Base.metadata knows about all tables thanks to the import
        Base.metadata.drop_all(eng)
        Base.metadata.create_all(eng)
    else:
        # This is safer for production runs, as it's idempotent.
        logger.info("Ensuring all tables exist (create if not present)...")
        Base.metadata.create_all(eng)


def get_or_create_folder(eng: sa.Engine, folder_id: int, folder_path: str) -> bool:
    """
    Ensures the parent FolderMetadata row exists before processing.
    Returns True on success, False on failure.
    """
    with eng.connect() as conn:
        try:
            conn.execute(
                FolderMetadata.__table__.insert(),
                # Use model column names for insert: 'FolderID' and 'FolderPath'
                {"FolderID": folder_id, "FolderPath": folder_path},
            )
            conn.commit()
            logger.debug(f"Created new FolderID {folder_id}")
            return True
        except sa.exc.IntegrityError:
            # This handles the case where the folder already exists.
            conn.rollback()
            logger.debug(f"FolderID {folder_id} already exists.")
            return True
        except Exception as e:
            logger.error(f"Failed to create FolderID {folder_id} in database: {e}")
            conn.rollback()
            return False
