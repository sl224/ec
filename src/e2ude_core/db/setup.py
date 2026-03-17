import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.schema import CreateSchema

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.base_session import Base

# Import models to populate `Base.metadata`
import e2ude_core.db.models  # noqa: F401
from e2ude_core.db.models import (
    ArtifactManifest,
    FileHashRegistry,
    FileMetadata,
    FolderMetadata,
    ProcessingJob,
    ProcessingSession,
)
from e2ude_core.registry import HANDLER_REGISTRY

logger = logging.getLogger(__name__)

FOLDER_LOOKUP_BATCH_SIZE = 1000


def _runtime_tables() -> list[sa.Table]:
    runtime_models = {
        FolderMetadata,
        FileHashRegistry,
        FileMetadata,
        ProcessingSession,
        ProcessingJob,
        ArtifactManifest,
    }

    for handler in HANDLER_REGISTRY.values():
        runtime_models.update(handler.expected_models)

    runtime_table_keys = {model.__table__.key for model in runtime_models}
    return [
        table
        for table in Base.metadata.sorted_tables
        if table.key in runtime_table_keys
    ]


def initialize_database(eng: sa.Engine, reset_tables: bool = False):
    """
    Ensures the necessary database schema exists and optionally resets tables.
    """
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

    tables_to_create = _runtime_tables()

    if reset_tables:
        if settings.database.type == "mssql":
            logger.error("Trying to reset tables when using mssql")
            raise Exception("Cannot reset tables when using mssql for safety.")
        logger.info("Resetting and creating database tables...")
        Base.metadata.drop_all(eng)
        Base.metadata.create_all(eng, tables=tables_to_create)
    else:
        logger.info("Ensuring all tables exist (create if not present)...")
        Base.metadata.create_all(eng, tables=tables_to_create)


def _iter_path_batches(paths: Iterable[str], batch_size: int | None = None):
    if batch_size is None:
        batch_size = FOLDER_LOOKUP_BATCH_SIZE

    batch: list[str] = []
    for path in paths:
        batch.append(path)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def _load_existing_folder_map(
    conn: sa.Connection, unique_paths: Iterable[str]
) -> Dict[str, int]:
    existing_map: Dict[str, int] = {}

    for batch in _iter_path_batches(unique_paths):
        stmt = sa.select(FolderMetadata.id, FolderMetadata.path).where(
            FolderMetadata.path.in_(batch)
        )
        for row in conn.execute(stmt):
            existing_map[row.path] = row.id

    return existing_map


def register_folders_bulk(eng: sa.Engine, zip_paths: List[Path]) -> Dict[Path, int]:
    """
    Registers folders in the DB.
    Returns a map of {Path: FolderID} for ALL provided paths.
    """
    if not zip_paths:
        return {}

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
                    "obj_path": zp,
                    "buno": buno,
                    "folder_datetime": dt,
                    "path": str(zp),
                }
            )
        except ValueError:
            logger.warning(f"Invalid date format in: {zp.name}")
            continue

    if not parsed_items:
        return {}

    unique_paths = list(dict.fromkeys(item["path"] for item in parsed_items))
    existing_map: Dict[str, int] = {}

    with eng.connect() as conn:
        if unique_paths:
            existing_map.update(_load_existing_folder_map(conn, unique_paths))

        to_insert = []
        seen_in_batch = set()

        for item in parsed_items:
            key = item["path"]

            if key in existing_map or key in seen_in_batch:
                continue

            seen_in_batch.add(key)
            to_insert.append(
                {
                    "buno": item["buno"],
                    "folder_datetime": item["folder_datetime"],
                    "path": item["path"],
                    "scan_version": 0,
                }
            )

        if to_insert:
            logger.info(f"Bulk inserting {len(to_insert)} new folders...")
            df_insert = pd.DataFrame(to_insert)
            sql_io.bulk_upload(
                df=df_insert, conn=conn, sa_table=FolderMetadata.__table__
            )
            conn.commit()

            existing_map.update(_load_existing_folder_map(conn, unique_paths))

    result_map = {}
    for item in parsed_items:
        key = item["path"]
        if key in existing_map:
            result_map[item["obj_path"]] = existing_map[key]

    return result_map
