import logging
import sqlalchemy as sa
from sqlalchemy.schema import CreateSchema
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import pandas as pd

from e2ude_core.config import settings
from e2ude_core.db.base_session import Base
from e2ude_core.db import access as sql_io

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
    Returns a map ONLY for folders that were newly inserted (incremental mode).
    """
    if not zip_paths:
        return {}

    # 1. Parse Metadata from Paths
    parsed_items = []
    for zp in zip_paths:
        # Regex looks for BUNO_YYYYMMDD_HHMMSS pattern
        match = re.search(r"([0-9]+)_([0-9]{8}_[0-9]{6})", zp.name)
        if not match:
            logger.warning(f"Could not parse BUNO/Date from: {zp.name}")
            continue

        buno, dt_str = match.groups()
        try:
            dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
            parsed_items.append(
                {
                    "obj_path": zp,  
                    "buno": buno,
                    "folder_datetime": dt,
                    "path": str(zp), 
                    "scan_version": 0,
                }
            )
        except ValueError:
            logger.warning(f"Invalid date format in: {zp.name}")
            continue

    if not parsed_items:
        return {}

    # 2. Fetch Existing IDs (Bulk Query) to determine what is NOT new
    unique_bunos = {p["buno"] for p in parsed_items}
    existing_keys = set() 

    with eng.connect() as conn:
        if unique_bunos:
            stmt = sa.select(
                FolderMetadata.buno, FolderMetadata.folder_datetime
            ).where(FolderMetadata.buno.in_(unique_bunos))

            for row in conn.execute(stmt):
                existing_keys.add((row.buno, row.folder_datetime))

        # 3. Diff & Prepare Inserts
        to_insert = []
        new_keys = set() # Track keys that we are about to insert

        for item in parsed_items:
            key = (item["buno"], item["folder_datetime"])

            if key in existing_keys:
                continue
            
            if key in new_keys: # Avoid duplicates within the same batch
                continue

            new_keys.add(key)
            to_insert.append(
                {
                    "buno": item["buno"],
                    "folder_datetime": item["folder_datetime"],
                    "path": item["path"],
                    "scan_version": item["scan_version"],
                }
            )

        # 4. Bulk Insert
        if to_insert:
            logger.info(f"Bulk inserting {len(to_insert)} new folders...")
            df_insert = pd.DataFrame(to_insert)
            sql_io.bulk_upload(
                df=df_insert, conn=conn, sa_table=FolderMetadata.__table__
            )
            conn.commit()

        # 5. Fetch IDs ONLY for the newly inserted keys
        # We query again, but we will filter the final map based on 'new_keys'
        if not new_keys:
            return {}

        # Fetch IDs for the BUNOs we just touched
        id_map = {}
        stmt = sa.select(
            FolderMetadata.id, FolderMetadata.buno, FolderMetadata.folder_datetime
        ).where(FolderMetadata.buno.in_(unique_bunos))

        for row in conn.execute(stmt):
            id_map[(row.buno, row.folder_datetime)] = row.id

    # 6. Build Result Map (FILTERED)
    result_map = {}
    for item in parsed_items:
        key = (item["buno"], item["folder_datetime"])
        
        # ONLY return if it was part of the 'new_keys' set
        if key in new_keys and key in id_map:
            result_map[item["obj_path"]] = id_map[key]

    return result_map