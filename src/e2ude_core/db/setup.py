import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, List

import sqlalchemy as sa
from sqlalchemy import bindparam
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateSchema

from e2ude_core.config import settings
from e2ude_core.db.base_session import Base
import e2ude_core.db.models  # noqa: F401
from e2ude_core.db.models import (
    ArchiveMetadata,
    ArchiveStateEnum,
    DiscoveryDirectoryMetadata,
    ArtifactManifest,
    FileHashRegistry,
    FileMetadata,
    ProcessingJob,
    ProcessingSession,
)
from e2ude_core.pipelines.scanner import SCANNER_VERSION
from e2ude_core.registry import CURRENT_HANDLER_GENERATION, HANDLER_REGISTRY
from e2ude_core.services.discovery import (
    DiscoveredArchive,
    DiscoveryDirectorySnapshot,
    KnownDiscoveryDirectory,
)

logger = logging.getLogger(__name__)

ARCHIVE_LOOKUP_BATCH_SIZE = 1000
ARCHIVE_NAME_PATTERN = re.compile(r"([0-9]+)_([0-9]{8}_[0-9]{6})")


def _runtime_tables() -> list[sa.Table]:
    runtime_models = {
        ArchiveMetadata,
        DiscoveryDirectoryMetadata,
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
    """Ensure the configured schema and runtime tables exist."""
    if settings.database.type == "mssql":
        from e2ude_core.db.base_session import DEFAULT_SCHEMA

        if not DEFAULT_SCHEMA:
            logger.error("MSSQL is selected but DEFAULT_SCHEMA is not set. Exiting.")
            raise SystemExit(1)

        logger.info("Ensuring MSSQL schema '%s' exists...", DEFAULT_SCHEMA)
        with eng.connect() as conn:
            if not conn.dialect.has_schema(conn, DEFAULT_SCHEMA):
                conn.execute(CreateSchema(DEFAULT_SCHEMA))
                logger.info("Schema '%s' created.", DEFAULT_SCHEMA)
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


def _iter_path_batches(
    paths: Iterable[str], batch_size: int | None = None
) -> Iterable[list[str]]:
    size = batch_size or ARCHIVE_LOOKUP_BATCH_SIZE
    batch: list[str] = []
    for path in paths:
        batch.append(path)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _load_existing_archive_rows(
    conn: sa.Connection, unique_paths: Iterable[str]
) -> Dict[str, sa.Row]:
    existing_map: Dict[str, sa.Row] = {}

    for batch in _iter_path_batches(unique_paths):
        stmt = sa.select(
            ArchiveMetadata.id,
            ArchiveMetadata.source_path,
            ArchiveMetadata.source_size_bytes,
            ArchiveMetadata.source_mtime_ns,
            ArchiveMetadata.required_scan_version,
            ArchiveMetadata.completed_scan_version,
            ArchiveMetadata.required_handler_generation,
            ArchiveMetadata.completed_handler_generation,
            ArchiveMetadata.state,
            ArchiveMetadata.work_reason,
        ).where(ArchiveMetadata.source_path.in_(batch))
        for row in conn.execute(stmt):
            existing_map[row.source_path] = row

    return existing_map


def _load_archive_id_map(
    conn: sa.Connection, unique_paths: Iterable[str]
) -> Dict[str, int]:
    id_map: Dict[str, int] = {}

    for batch in _iter_path_batches(unique_paths):
        stmt = sa.select(ArchiveMetadata.id, ArchiveMetadata.source_path).where(
            ArchiveMetadata.source_path.in_(batch)
        )
        for row in conn.execute(stmt):
            id_map[row.source_path] = row.id

    return id_map


def load_directory_scan_cache(
    eng: sa.Engine,
    *,
    root_path: Path | None = None,
) -> Dict[str, KnownDiscoveryDirectory]:
    with eng.connect() as conn:
        stmt = sa.select(
            DiscoveryDirectoryMetadata.path,
            DiscoveryDirectoryMetadata.mtime_ns,
            DiscoveryDirectoryMetadata.contains_archives,
        )
        if root_path is not None:
            prefix = f"{root_path}%"
            stmt = stmt.where(DiscoveryDirectoryMetadata.path.like(prefix))
        return {
            row.path: KnownDiscoveryDirectory(
                path=Path(row.path),
                mtime_ns=row.mtime_ns,
                contains_archives=row.contains_archives,
            )
            for row in conn.execute(stmt)
        }


def record_directory_snapshots(
    eng: sa.Engine,
    snapshots: Iterable[DiscoveryDirectorySnapshot],
    *,
    missing_paths: Iterable[Path] = (),
) -> None:
    snapshot_list = list(snapshots)
    missing_path_list = [str(path) for path in missing_paths]
    if not snapshot_list and not missing_path_list:
        return

    seen_at = datetime.now(UTC).replace(tzinfo=None)

    with eng.begin() as conn:
        for snapshot in snapshot_list:
            normalized_path = str(snapshot.path)
            update_values = {
                "mtime_ns": snapshot.mtime_ns,
                "contains_archives": snapshot.contains_archives,
                "last_checked_at": seen_at,
            }
            insert_values = {
                "path": normalized_path,
                "mtime_ns": snapshot.mtime_ns,
                "contains_archives": snapshot.contains_archives,
                "last_checked_at": seen_at,
                "last_scanned_at": seen_at if snapshot.scanned else None,
            }
            if snapshot.scanned:
                update_values["last_scanned_at"] = seen_at

            result = conn.execute(
                sa.update(DiscoveryDirectoryMetadata)
                .where(DiscoveryDirectoryMetadata.path == normalized_path)
                .values(**update_values)
            )

            if result.rowcount:
                continue

            try:
                conn.execute(
                    sa.insert(DiscoveryDirectoryMetadata).values(**insert_values)
                )
            except IntegrityError:
                conn.execute(
                    sa.update(DiscoveryDirectoryMetadata)
                    .where(DiscoveryDirectoryMetadata.path == normalized_path)
                    .values(**update_values)
                )

        if missing_path_list:
            for batch in _iter_path_batches(missing_path_list):
                conn.execute(
                    DiscoveryDirectoryMetadata.__table__.delete().where(
                        DiscoveryDirectoryMetadata.path.in_(batch)
                    )
                )


def _coerce_discovered_archive(record: DiscoveredArchive | Path) -> DiscoveredArchive:
    if isinstance(record, DiscoveredArchive):
        return record
    if record.exists():
        stat = record.stat()
        size_bytes = stat.st_size
        mtime_ns = stat.st_mtime_ns
    else:
        size_bytes = 0
        mtime_ns = 0
    return DiscoveredArchive(
        path=record,
        size_bytes=size_bytes,
        mtime_ns=mtime_ns,
    )


def _parse_archive_record(record: DiscoveredArchive | Path) -> dict | None:
    record = _coerce_discovered_archive(record)
    match = ARCHIVE_NAME_PATTERN.search(record.path.name)
    if not match:
        logger.warning("Could not parse BUNO/Date from: %s", record.path.name)
        return None

    buno, dt_str = match.groups()
    try:
        archive_datetime = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
    except ValueError:
        logger.warning("Invalid date format in: %s", record.path.name)
        return None

    return {
        "obj_path": record.path,
        "buno": buno,
        "archive_datetime": archive_datetime,
        "source_path": str(record.path),
        "source_size_bytes": record.size_bytes,
        "source_mtime_ns": record.mtime_ns,
    }


def _build_archive_insert(item: dict, seen_at: datetime) -> dict:
    return {
        "buno": item["buno"],
        "archive_datetime": item["archive_datetime"],
        "source_path": item["source_path"],
        "source_size_bytes": item["source_size_bytes"],
        "source_mtime_ns": item["source_mtime_ns"],
        "first_seen_at": seen_at,
        "last_seen_at": seen_at,
        "is_present": True,
        "required_scan_version": SCANNER_VERSION,
        "completed_scan_version": 0,
        "required_handler_generation": CURRENT_HANDLER_GENERATION,
        "completed_handler_generation": None,
        "state": ArchiveStateEnum.NEEDS_SCAN,
        "work_reason": "New archive discovered",
    }


def _build_archive_update(item: dict, existing: sa.Row, seen_at: datetime) -> dict:
    changed_source = (
        existing.source_size_bytes != item["source_size_bytes"]
        or existing.source_mtime_ns != item["source_mtime_ns"]
    )

    needs_scan = changed_source or existing.completed_scan_version < SCANNER_VERSION
    needs_processing = (
        existing.completed_handler_generation != CURRENT_HANDLER_GENERATION
    )

    next_state = existing.state
    next_reason = existing.work_reason
    completed_scan_version = existing.completed_scan_version
    completed_handler_generation = existing.completed_handler_generation

    if changed_source:
        next_state = ArchiveStateEnum.NEEDS_SCAN
        next_reason = "Source archive changed"
        completed_scan_version = 0
        completed_handler_generation = None
    elif needs_scan:
        next_state = ArchiveStateEnum.NEEDS_SCAN
        next_reason = "Scanner version changed"
    elif needs_processing and existing.state == ArchiveStateEnum.UP_TO_DATE:
        next_state = ArchiveStateEnum.NEEDS_PROCESSING
        next_reason = "Handler generation changed"
    elif existing.state == ArchiveStateEnum.NEEDS_PROCESSING:
        next_state = ArchiveStateEnum.NEEDS_PROCESSING
    elif existing.state == ArchiveStateEnum.NEEDS_SCAN:
        next_state = ArchiveStateEnum.NEEDS_SCAN
    else:
        next_state = ArchiveStateEnum.UP_TO_DATE
        next_reason = None

    return {
        "b_id": existing.id,
        "b_buno": item["buno"],
        "b_archive_datetime": item["archive_datetime"],
        "b_source_size_bytes": item["source_size_bytes"],
        "b_source_mtime_ns": item["source_mtime_ns"],
        "b_last_seen_at": seen_at,
        "b_is_present": True,
        "b_required_scan_version": SCANNER_VERSION,
        "b_completed_scan_version": completed_scan_version,
        "b_required_handler_generation": CURRENT_HANDLER_GENERATION,
        "b_completed_handler_generation": completed_handler_generation,
        "b_state": next_state,
        "b_work_reason": next_reason,
        "b_last_error_at": None,
        "b_last_error_message": None,
    }


def _insert_new_archives(conn: sa.Connection, rows: list[dict]) -> None:
    if not rows:
        return

    insert_stmt = sa.insert(ArchiveMetadata)
    for row in rows:
        try:
            conn.execute(insert_stmt.values(**row))
        except IntegrityError:
            # A concurrent refresh may have inserted this source_path after the
            # prefetch step. Treat that as a benign race and let the final id
            # reload pick up the existing row.
            continue


def _mark_absent_archives_for_scanned_directories(
    conn: sa.Connection,
    scanned_directory_paths: Iterable[str],
    seen_source_paths: set[str],
) -> int:
    absent_ids: list[int] = []
    for dir_path in scanned_directory_paths:
        prefix = f"{dir_path}{os.sep}%"
        stmt = sa.select(ArchiveMetadata.id, ArchiveMetadata.source_path).where(
            ArchiveMetadata.source_path.like(prefix)
        )
        for row in conn.execute(stmt):
            source_path = row.source_path
            if str(Path(source_path).parent) != dir_path:
                continue
            if source_path not in seen_source_paths:
                absent_ids.append(row.id)

    if absent_ids:
        for i in range(0, len(absent_ids), ARCHIVE_LOOKUP_BATCH_SIZE):
            batch = absent_ids[i : i + ARCHIVE_LOOKUP_BATCH_SIZE]
            conn.execute(
                sa.update(ArchiveMetadata)
                .where(ArchiveMetadata.id.in_(batch))
                .values(
                    is_present=False,
                    state=ArchiveStateEnum.UP_TO_DATE,
                    work_reason="Archive missing from scanned directory",
                )
            )
    return len(absent_ids)


def _mark_absent_archives_for_missing_directories(
    conn: sa.Connection, missing_directory_paths: Iterable[str]
) -> int:
    absent_total = 0
    for dir_path in missing_directory_paths:
        prefix = f"{dir_path}{os.sep}%"
        stmt = sa.select(ArchiveMetadata.id, ArchiveMetadata.source_path).where(
            ArchiveMetadata.source_path.like(prefix)
        )
        absent_ids = [
            row.id
            for row in conn.execute(stmt)
            if str(Path(row.source_path).parent) == dir_path
        ]
        if not absent_ids:
            continue
        absent_total += len(absent_ids)

        for i in range(0, len(absent_ids), ARCHIVE_LOOKUP_BATCH_SIZE):
            batch = absent_ids[i : i + ARCHIVE_LOOKUP_BATCH_SIZE]
            conn.execute(
                sa.update(ArchiveMetadata)
                .where(ArchiveMetadata.id.in_(batch))
                .values(
                    is_present=False,
                    state=ArchiveStateEnum.UP_TO_DATE,
                    work_reason="Archive directory missing from source share",
                )
            )
    return absent_total


def register_archives_bulk(
    eng: sa.Engine,
    discovered_archives: List[DiscoveredArchive | Path],
    *,
    scanned_directory_paths: Iterable[Path] = (),
    missing_directory_paths: Iterable[Path] = (),
) -> Dict[Path, int]:
    """
    Upsert the discovered source archives into the canonical archive inventory.

    Returns a map of {Path: archive_id} for all discovered and parseable archives.
    """
    if not discovered_archives:
        return {}

    parsed_items = []
    for record in discovered_archives:
        parsed = _parse_archive_record(record)
        if parsed is not None:
            parsed_items.append(parsed)

    normalized_scanned_dirs = {str(path) for path in scanned_directory_paths}
    normalized_missing_dirs = {str(path) for path in missing_directory_paths}
    if not parsed_items and not normalized_scanned_dirs and not normalized_missing_dirs:
        return {}

    unique_paths = list(dict.fromkeys(item["source_path"] for item in parsed_items))
    seen_at = datetime.now(UTC).replace(tzinfo=None)
    seen_source_paths = set(unique_paths)

    with eng.begin() as conn:
        existing_map = _load_existing_archive_rows(conn, unique_paths)

        to_insert: list[dict] = []
        to_update: list[dict] = []
        seen_in_batch: set[str] = set()
        changed_existing_count = 0
        unchanged_existing_count = 0

        for item in parsed_items:
            path_key = item["source_path"]
            if path_key in seen_in_batch:
                continue
            seen_in_batch.add(path_key)

            existing = existing_map.get(path_key)
            if existing is None:
                to_insert.append(_build_archive_insert(item, seen_at))
                continue

            if (
                existing.source_size_bytes != item["source_size_bytes"]
                or existing.source_mtime_ns != item["source_mtime_ns"]
            ):
                changed_existing_count += 1
            else:
                unchanged_existing_count += 1
            to_update.append(_build_archive_update(item, existing, seen_at))

        if to_insert:
            logger.info("Upserting %s new archives...", len(to_insert))
            _insert_new_archives(conn, to_insert)

        if to_update:
            stmt = (
                sa.update(ArchiveMetadata)
                .where(ArchiveMetadata.id == bindparam("b_id"))
                .values(
                    buno=bindparam("b_buno"),
                    archive_datetime=bindparam("b_archive_datetime"),
                    source_size_bytes=bindparam("b_source_size_bytes"),
                    source_mtime_ns=bindparam("b_source_mtime_ns"),
                    last_seen_at=bindparam("b_last_seen_at"),
                    is_present=bindparam("b_is_present"),
                    required_scan_version=bindparam("b_required_scan_version"),
                    completed_scan_version=bindparam("b_completed_scan_version"),
                    required_handler_generation=bindparam(
                        "b_required_handler_generation"
                    ),
                    completed_handler_generation=bindparam(
                        "b_completed_handler_generation"
                    ),
                    state=bindparam("b_state"),
                    work_reason=bindparam("b_work_reason"),
                    last_error_at=bindparam("b_last_error_at"),
                    last_error_message=bindparam("b_last_error_message"),
                )
            )
            conn.execute(stmt, to_update)

        absent_from_scanned_dirs = 0
        if normalized_scanned_dirs:
            absent_from_scanned_dirs = _mark_absent_archives_for_scanned_directories(
                conn,
                normalized_scanned_dirs,
                seen_source_paths,
            )
        absent_from_missing_dirs = 0
        if normalized_missing_dirs:
            absent_from_missing_dirs = _mark_absent_archives_for_missing_directories(
                conn,
                normalized_missing_dirs,
            )

        archive_id_map = _load_archive_id_map(conn, unique_paths)

    logger.info(
        "Archive registration complete. discovered=%s inserted=%s "
        "existing_changed=%s existing_unchanged=%s absent_from_scanned_dirs=%s "
        "absent_from_missing_dirs=%s",
        len(unique_paths),
        len(to_insert),
        changed_existing_count,
        unchanged_existing_count,
        absent_from_scanned_dirs,
        absent_from_missing_dirs,
    )

    result_map: Dict[Path, int] = {}
    for item in parsed_items:
        archive_id = archive_id_map.get(item["source_path"])
        if archive_id is not None:
            result_map[item["obj_path"]] = archive_id

    return result_map
