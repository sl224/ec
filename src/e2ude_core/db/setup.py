import logging
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
from e2ude_core.runtime_files import CURRENT_HANDLER_GENERATION, HANDLED_FILE_SPECS
from e2ude_core.orchestration.state import summarize_archive_facts
from e2ude_core.services.discovery import (
    DiscoveredArchive,
    DiscoveryDirectorySnapshot,
    KnownDiscoveryDirectory,
)

logger = logging.getLogger(__name__)

ARCHIVE_LOOKUP_BATCH_SIZE = 1000
ARCHIVE_INSERT_BATCH_SIZE = 1000
DIRECTORY_SNAPSHOT_BATCH_SIZE = 1000
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

    for spec in HANDLED_FILE_SPECS:
        runtime_models.update(spec.expected_models)

    runtime_table_keys = {model.__table__.key for model in runtime_models}
    return [
        table
        for table in Base.metadata.sorted_tables
        if table.key in runtime_table_keys
    ]


def _qualified_table_name(table: sa.Table) -> str:
    if table.schema:
        return f"{table.schema}.{table.name}"
    return table.name


def _missing_runtime_columns(eng: sa.Engine, tables: list[sa.Table]) -> list[str]:
    inspector = sa.inspect(eng)
    missing: list[str] = []

    for table in tables:
        try:
            actual_columns = {
                column["name"]
                for column in inspector.get_columns(table.name, table.schema)
            }
        except sa.exc.NoSuchTableError:
            missing.append(f"{_qualified_table_name(table)}.*")
            continue

        for column in table.columns:
            if column.name not in actual_columns:
                missing.append(f"{_qualified_table_name(table)}.{column.name}")

    return missing


def _validate_runtime_schema(eng: sa.Engine, tables: list[sa.Table]) -> None:
    missing_columns = _missing_runtime_columns(eng, tables)
    if not missing_columns:
        return

    missing_summary = ", ".join(sorted(missing_columns))
    raise RuntimeError(
        "Database schema is out of date for this code version. "
        "SQLAlchemy create_all() only creates missing tables; it does not add "
        "columns to existing tables. Missing runtime columns: "
        f"{missing_summary}. Rebuild the target schema or run an explicit migration "
        "before starting the pipeline."
    )


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

    _validate_runtime_schema(eng, tables_to_create)


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
            ArchiveMetadata.is_present,
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


def _load_existing_directory_paths(
    conn: sa.Connection, paths: Iterable[str]
) -> set[str]:
    existing: set[str] = set()
    for batch in _iter_path_batches(paths, DIRECTORY_SNAPSHOT_BATCH_SIZE):
        stmt = sa.select(DiscoveryDirectoryMetadata.path).where(
            DiscoveryDirectoryMetadata.path.in_(batch)
        )
        existing.update(row.path for row in conn.execute(stmt))
    return existing


def _update_directory_snapshots(conn: sa.Connection, rows: list[dict]) -> None:
    if not rows:
        return

    scanned_rows = [row for row in rows if row["b_last_scanned_at"] is not None]
    unscanned_rows = [row for row in rows if row["b_last_scanned_at"] is None]

    base_values = {
        "mtime_ns": bindparam("b_mtime_ns"),
        "contains_archives": bindparam("b_contains_archives"),
        "last_checked_at": bindparam("b_last_checked_at"),
    }

    if unscanned_rows:
        stmt = (
            sa.update(DiscoveryDirectoryMetadata)
            .where(DiscoveryDirectoryMetadata.path == bindparam("b_path"))
            .values(**base_values)
        )
        for i in range(0, len(unscanned_rows), DIRECTORY_SNAPSHOT_BATCH_SIZE):
            conn.execute(stmt, unscanned_rows[i : i + DIRECTORY_SNAPSHOT_BATCH_SIZE])

    if scanned_rows:
        stmt = (
            sa.update(DiscoveryDirectoryMetadata)
            .where(DiscoveryDirectoryMetadata.path == bindparam("b_path"))
            .values(
                **base_values,
                last_scanned_at=bindparam("b_last_scanned_at"),
            )
        )
        for i in range(0, len(scanned_rows), DIRECTORY_SNAPSHOT_BATCH_SIZE):
            conn.execute(stmt, scanned_rows[i : i + DIRECTORY_SNAPSHOT_BATCH_SIZE])


def _directory_insert_to_update_row(row: dict) -> dict:
    return {
        "b_path": row["path"],
        "b_mtime_ns": row["mtime_ns"],
        "b_contains_archives": row["contains_archives"],
        "b_last_checked_at": row["last_checked_at"],
        "b_last_scanned_at": row["last_scanned_at"],
    }


def _insert_directory_snapshots(conn: sa.Connection, rows: list[dict]) -> None:
    if not rows:
        return

    insert_stmt = sa.insert(DiscoveryDirectoryMetadata)
    for i in range(0, len(rows), DIRECTORY_SNAPSHOT_BATCH_SIZE):
        batch = rows[i : i + DIRECTORY_SNAPSHOT_BATCH_SIZE]
        try:
            conn.execute(insert_stmt, batch)
        except IntegrityError:
            logger.info(
                "Directory snapshot batch insert hit an integrity race; "
                "falling back to row-by-row insert for this %s-row batch.",
                len(batch),
            )
            for row in batch:
                try:
                    conn.execute(insert_stmt.values(**row))
                except IntegrityError:
                    _update_directory_snapshots(
                        conn, [_directory_insert_to_update_row(row)]
                    )


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
        snapshot_by_path = {str(snapshot.path): snapshot for snapshot in snapshot_list}
        existing_paths = _load_existing_directory_paths(conn, snapshot_by_path.keys())

        to_insert: list[dict] = []
        to_update: list[dict] = []

        for normalized_path, snapshot in snapshot_by_path.items():
            update_row = {
                "b_path": normalized_path,
                "b_mtime_ns": snapshot.mtime_ns,
                "b_contains_archives": snapshot.contains_archives,
                "b_last_checked_at": seen_at,
                "b_last_scanned_at": seen_at if snapshot.scanned else None,
            }
            if normalized_path in existing_paths:
                to_update.append(update_row)
                continue

            to_insert.append(
                {
                    "path": normalized_path,
                    "mtime_ns": snapshot.mtime_ns,
                    "contains_archives": snapshot.contains_archives,
                    "last_checked_at": seen_at,
                    "last_scanned_at": seen_at if snapshot.scanned else None,
                }
            )

        _update_directory_snapshots(conn, to_update)
        _insert_directory_snapshots(conn, to_insert)

        if missing_path_list:
            for batch in _iter_path_batches(
                missing_path_list, DIRECTORY_SNAPSHOT_BATCH_SIZE
            ):
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

    completed_scan_version = 0 if changed_source else existing.completed_scan_version
    completed_handler_generation = (
        None if changed_source else existing.completed_handler_generation
    )
    if changed_source:
        reason = "Source archive changed"
    elif existing.required_scan_version != SCANNER_VERSION:
        reason = "Scanner version changed"
    elif existing.required_handler_generation != CURRENT_HANDLER_GENERATION:
        reason = "Handler generation changed"
    else:
        reason = existing.work_reason

    summary = summarize_archive_facts(
        is_present=True,
        completed_scan_version=completed_scan_version,
        required_scan_version=SCANNER_VERSION,
        completed_handler_generation=completed_handler_generation,
        required_handler_generation=CURRENT_HANDLER_GENERATION,
        stored_state=existing.state,
        work_reason=reason,
    )
    next_reason = None if summary.status == ArchiveStateEnum.UP_TO_DATE else reason

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
        "b_state": summary.status,
        "b_work_reason": next_reason,
        "b_last_error_at": None,
        "b_last_error_message": None,
    }


def _insert_new_archives(conn: sa.Connection, rows: list[dict]) -> None:
    if not rows:
        return

    insert_stmt = sa.insert(ArchiveMetadata)
    inserted_count = 0

    for i in range(0, len(rows), ARCHIVE_INSERT_BATCH_SIZE):
        batch = rows[i : i + ARCHIVE_INSERT_BATCH_SIZE]
        try:
            conn.execute(insert_stmt, batch)
        except IntegrityError:
            logger.info(
                "Archive batch insert hit an integrity race; falling back to "
                "row-by-row insert for this %s-row batch.",
                len(batch),
            )
            for row in batch:
                try:
                    conn.execute(insert_stmt.values(**row))
                except IntegrityError:
                    # A concurrent refresh may have inserted this source_path after the
                    # prefetch step. Treat that as a benign race and let the final id
                    # reload pick up the existing row.
                    continue

        inserted_count += len(batch)
        logger.info("Inserted %s/%s new archive rows...", inserted_count, len(rows))


def _mark_absent_archives(
    conn: sa.Connection,
    scanned_directory_paths: Iterable[str],
    missing_directory_paths: Iterable[str],
    seen_source_paths: set[str],
) -> tuple[int, int]:
    scanned_dirs = set(scanned_directory_paths)
    missing_dirs = set(missing_directory_paths)
    if not scanned_dirs and not missing_dirs:
        return 0, 0

    absent_from_scanned: list[int] = []
    absent_from_missing: list[int] = []
    rows = conn.execute(
        sa.select(ArchiveMetadata.id, ArchiveMetadata.source_path).where(
            ArchiveMetadata.is_present == sa.true()
        )
    )
    for row in rows:
        source_path = row.source_path
        parent_dir = str(Path(source_path).parent)
        if parent_dir in missing_dirs:
            absent_from_missing.append(row.id)
        elif parent_dir in scanned_dirs and source_path not in seen_source_paths:
            absent_from_scanned.append(row.id)

    updates = (
        (
            absent_from_scanned,
            "Archive missing from scanned directory",
        ),
        (
            absent_from_missing,
            "Archive directory missing from source share",
        ),
    )
    for absent_ids, reason in updates:
        for i in range(0, len(absent_ids), ARCHIVE_LOOKUP_BATCH_SIZE):
            conn.execute(
                sa.update(ArchiveMetadata)
                .where(
                    ArchiveMetadata.id.in_(
                        absent_ids[i : i + ARCHIVE_LOOKUP_BATCH_SIZE]
                    )
                )
                .values(
                    is_present=False,
                    state=ArchiveStateEnum.UP_TO_DATE,
                    work_reason=reason,
                )
            )

    return len(absent_from_scanned), len(absent_from_missing)


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
        updated_existing_count = 0
        skipped_existing_count = 0

        for item in parsed_items:
            path_key = item["source_path"]
            if path_key in seen_in_batch:
                continue
            seen_in_batch.add(path_key)

            existing = existing_map.get(path_key)
            if existing is None:
                to_insert.append(_build_archive_insert(item, seen_at))
                continue

            update_row = _build_archive_update(item, existing, seen_at)
            update_needed = (
                existing.source_size_bytes != update_row["b_source_size_bytes"]
                or existing.source_mtime_ns != update_row["b_source_mtime_ns"]
                or not existing.is_present
                or existing.required_scan_version
                != update_row["b_required_scan_version"]
                or existing.completed_scan_version
                != update_row["b_completed_scan_version"]
                or existing.required_handler_generation
                != update_row["b_required_handler_generation"]
                or existing.completed_handler_generation
                != update_row["b_completed_handler_generation"]
                or existing.state != update_row["b_state"]
                or existing.work_reason != update_row["b_work_reason"]
            )
            if not update_needed:
                skipped_existing_count += 1
                continue

            updated_existing_count += 1
            to_update.append(update_row)

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

        logger.info(
            "Reconciling archive presence for %s scanned dirs and %s missing dirs...",
            len(normalized_scanned_dirs),
            len(normalized_missing_dirs),
        )
        absent_from_scanned_dirs, absent_from_missing_dirs = _mark_absent_archives(
            conn,
            normalized_scanned_dirs,
            normalized_missing_dirs,
            seen_source_paths,
        )

        logger.info("Loading IDs for %s discovered archives...", len(unique_paths))
        archive_id_map = _load_archive_id_map(conn, unique_paths)

    logger.info(
        "Archive registration complete. discovered=%s inserted=%s "
        "existing_updated=%s existing_skipped=%s absent_from_scanned_dirs=%s "
        "absent_from_missing_dirs=%s",
        len(unique_paths),
        len(to_insert),
        updated_existing_count,
        skipped_existing_count,
        absent_from_scanned_dirs,
        absent_from_missing_dirs,
    )

    result_map: Dict[Path, int] = {}
    for item in parsed_items:
        archive_id = archive_id_map.get(item["source_path"])
        if archive_id is not None:
            result_map[item["obj_path"]] = archive_id

    return result_map
