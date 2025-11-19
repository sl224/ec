import logging
import sqlalchemy as sa
from sqlalchemy.schema import CreateSchema
import re
from datetime import datetime
from pathlib import Path

from e2ude_core.config import settings
from e2ude_core.db.base_session import Base

# Import models to populate `Base.metadata` with table definitions
import e2ude_core.db.models  # noqa: F401
from e2ude_core.db.models import FolderMetadata

logger = logging.getLogger(__name__)


def initialize_database(eng: sa.Engine, reset_tables: bool = False):
    """
    Ensures the necessary database schema exists (for MSSQL)
    and optionally resets all tables.
    """
    # 1. Schema Creation (MSSQL-specific)
    if settings.database.type == "mssql":
        from e2ude_core.db.base_session import DEFAULT_SCHEMA

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


def get_or_create_folder(eng: sa.Engine, zip_path: Path) -> int | None:
    """
    Ensures the parent FolderMetadata row exists before processing.
    Returns the folder's ID on success, None on failure.
    """
    match = re.search(r"([0-9]+)_([0-9]{8}_[0-9]{6})", zip_path.name)
    if not match:
        logging.warning(f"Could not strip info from {zip_path}")
        return None, None
    buno, dt_str = match.groups()
    dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
    with eng.connect() as conn:
        try:
            # First, try to find the existing folder by its unique buno/datetime combination.
            existing_id = conn.execute(
                sa.select(FolderMetadata.id).where(
                    FolderMetadata.buno == buno, FolderMetadata.folder_datetime == dt
                )
            ).scalar_one_or_none()

            if existing_id:
                logger.debug(
                    f"Folder for buno '{buno}' at '{dt}' already exists with ID {existing_id}."
                )
                return existing_id

            # If it doesn't exist, insert it and get the new ID.
            result = conn.execute(
                sa.insert(FolderMetadata).values(
                    path=str(zip_path), buno=buno, folder_datetime=dt
                )
            )
            conn.commit()
            new_id = result.inserted_primary_key[0]
            logger.debug(f"Created new FolderMetadata record with ID {new_id}")
            return new_id
        except sa.exc.IntegrityError:  # Catches unique constraint violations
            # This handles a race condition where another process inserted it.
            conn.rollback()
            logger.warning(
                f"Race condition on insert for path '{zip_path}', re-querying."
            )
            return get_or_create_folder(eng, zip_path)
        except Exception as e:
            logger.error(
                f"Failed to get or create FolderMetadata for path '{zip_path}': {e}",
                exc_info=True,
            )
            conn.rollback()
            return None
