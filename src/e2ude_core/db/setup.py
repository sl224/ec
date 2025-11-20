import logging
import sqlalchemy as sa
from sqlalchemy.schema import CreateSchema
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict

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


def register_folders_bulk(eng: sa.Engine, zip_paths: List[Path]) -> Dict[Path, int]:
    """
    Optimized bulk registration of folders.

    Strategy:
    1. Parse all local paths to get (Buno, Date) keys.
    2. Fetch ALL existing IDs for those Bunos from DB in one query.
    3. Calculate missing folders in memory.
    4. Bulk Insert missing folders.
    5. Return a map of {InputPath -> FolderID}.
    """
    if not zip_paths:
        return {}

    # 1. Parse Metadata from Paths
    # We store `obj_path` to map the result back to the exact input object
    parsed_items = []

    for zp in zip_paths:
        match = re.search(r"([0-9]+)_([0-9]{8}_[0-9]{6})", zp.name)
        if not match:
            logger.warning(f"Could not parse BUNO/Date from: {zp.name}")
            continue

        buno, dt_str = match.groups()
        try:
            dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
            parsed_items.append(
                {
                    "obj_path": zp,  # Key for the returned map
                    "buno": buno,
                    "folder_datetime": dt,
                    "path": str(zp),  # String for DB insert
                    "scan_version": 0,  # Default for new folders
                }
            )
        except ValueError:
            logger.warning(f"Invalid date format in: {zp.name}")
            continue

    if not parsed_items:
        return {}

    # 2. Fetch Existing IDs (Bulk Query)
    # Filter by the BUNOs present in our list to reduce query scope
    unique_bunos = {p["buno"] for p in parsed_items}
    existing_map = {}  # Key: (buno, folder_datetime) -> Value: id

    with eng.connect() as conn:
        if unique_bunos:
            stmt = sa.select(
                FolderMetadata.id, FolderMetadata.buno, FolderMetadata.folder_datetime
            ).where(FolderMetadata.buno.in_(unique_bunos))

            for row in conn.execute(stmt):
                existing_map[(row.buno, row.folder_datetime)] = row.id

        # 3. Diff & Prepare Inserts
        to_insert = []
        seen_in_batch = set()

        for item in parsed_items:
            key = (item["buno"], item["folder_datetime"])

            # If already in DB, we don't need to insert
            if key in existing_map:
                continue

            # If we already queued this folder for insert in this batch (duplicate inputs), skip
            if key in seen_in_batch:
                continue

            seen_in_batch.add(key)

            # Prepare record for bulk insert
            to_insert.append(
                {
                    "buno": item["buno"],
                    "folder_datetime": item["folder_datetime"],
                    "path": item["path"],
                    "scan_version": item["scan_version"],
                }
            )

        # 4. Bulk Insert (If needed)
        if to_insert:
            logger.info(f"Bulk inserting {len(to_insert)} new folders...")
            conn.execute(sa.insert(FolderMetadata), to_insert)
            conn.commit()

            # 5. Re-fetch IDs for the newly inserted items
            # Simplest robust way across different DBs (vs RETURNING) is to re-query the keys
            stmt = sa.select(
                FolderMetadata.id, FolderMetadata.buno, FolderMetadata.folder_datetime
            ).where(FolderMetadata.buno.in_(unique_bunos))

            for row in conn.execute(stmt):
                existing_map[(row.buno, row.folder_datetime)] = row.id

    # 6. Build Result Map
    # Map the original Path objects to their IDs
    result_map = {}
    for item in parsed_items:
        key = (item["buno"], item["folder_datetime"])
        if key in existing_map:
            result_map[item["obj_path"]] = existing_map[key]

    return result_map
