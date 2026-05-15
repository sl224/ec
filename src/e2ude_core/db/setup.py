import logging
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, List

import sqlalchemy as sa
from sqlalchemy import bindparam
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.schema import CreateSchema

from e2ude_core.config import settings
from e2ude_core.db.base_session import Base
import e2ude_core.db.models  # noqa: F401
from e2ude_core.db.models import (
    ArchiveMetadata,
    ArtifactManifest,
    FileMetadata,
    ProcessingJob,
    ProcessingSession,
)
from e2ude_core.runtime_files import HANDLED_FILE_SPECS
from e2ude_core.services.discovery import DiscoveredArchive

logger = logging.getLogger(__name__)

ARCHIVE_LOOKUP_BATCH_SIZE = 1000
ARCHIVE_IDENTITY_LOOKUP_BATCH_SIZE = 500
ARCHIVE_INSERT_BATCH_SIZE = 1000
MSSQL_ARCHIVE_STAGING_BATCH_SIZE = 10000
ARCHIVE_REGISTRATION_DEADLOCK_RETRIES = 4
ARCHIVE_REGISTRATION_DEADLOCK_DELAY_SECONDS = 0.2
ARCHIVE_NAME_PATTERN = re.compile(
    r"^(?P<archive_key>(?P<buno>[0-9]+)_(?P<dt>[0-9]{8}_[0-9]{6})(?:_[0-9]+)?)_TransportRSM",
    re.IGNORECASE,
)


def _runtime_tables() -> list[sa.Table]:
    runtime_models = {
        ArchiveMetadata,
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


def _iter_batches(values: Iterable, batch_size: int | None = None) -> Iterable[list]:
    size = batch_size or ARCHIVE_LOOKUP_BATCH_SIZE
    batch: list = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _item_value(item: dict | sa.Row, name: str):
    if isinstance(item, dict):
        return item[name]
    return getattr(item, name)


def _locator_key(path: Path | str) -> str:
    return str(Path(path)).casefold()


def _archive_locator_key(item: dict | sa.Row) -> str:
    return _item_value(item, "locator_key")


def _load_existing_archive_rows(
    conn: sa.Connection, locator_keys: Iterable[str]
) -> Dict[str, sa.Row]:
    existing_map: Dict[str, sa.Row] = {}

    for batch in _iter_batches(locator_keys, ARCHIVE_IDENTITY_LOOKUP_BATCH_SIZE):
        stmt = sa.select(
            ArchiveMetadata.id,
            ArchiveMetadata.archive_key,
            ArchiveMetadata.buno,
            ArchiveMetadata.archive_datetime,
            ArchiveMetadata.locator_key,
            ArchiveMetadata.locator_path,
            ArchiveMetadata.locator_size_bytes,
            ArchiveMetadata.locator_mtime_ns,
            ArchiveMetadata.catalog_version,
            ArchiveMetadata.is_present,
        ).where(ArchiveMetadata.locator_key.in_(batch))
        for row in conn.execute(stmt):
            key = _archive_locator_key(row)
            if key in batch:
                existing_map[key] = row

    return existing_map


def _load_archive_id_map(
    conn: sa.Connection, locator_keys: Iterable[str]
) -> Dict[str, int]:
    id_map: Dict[str, int] = {}

    for batch in _iter_batches(locator_keys, ARCHIVE_IDENTITY_LOOKUP_BATCH_SIZE):
        stmt = sa.select(
            ArchiveMetadata.id,
            ArchiveMetadata.locator_key,
        ).where(ArchiveMetadata.locator_key.in_(batch))
        for row in conn.execute(stmt):
            key = _archive_locator_key(row)
            if key in batch:
                id_map[key] = row.id

    return id_map


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

    buno = match.group("buno")
    dt_str = match.group("dt")
    try:
        archive_datetime = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
    except ValueError:
        logger.warning("Invalid date format in: %s", record.path.name)
        return None

    return {
        "obj_path": record.path,
        "archive_key": match.group("archive_key"),
        "buno": buno,
        "archive_datetime": archive_datetime,
        "locator_key": _locator_key(record.path),
        "locator_path": str(record.path),
        "locator_size_bytes": record.size_bytes,
        "locator_mtime_ns": record.mtime_ns,
    }


def _build_archive_insert(item: dict, seen_at: datetime) -> dict:
    return {
        "archive_key": item["archive_key"],
        "buno": item["buno"],
        "archive_datetime": item["archive_datetime"],
        "locator_key": item["locator_key"],
        "locator_path": item["locator_path"],
        "locator_size_bytes": item["locator_size_bytes"],
        "locator_mtime_ns": item["locator_mtime_ns"],
        "first_seen_at": seen_at,
        "last_seen_at": seen_at,
        "is_present": True,
        "catalog_version": 0,
    }


def _build_archive_update(item: dict, existing: sa.Row, seen_at: datetime) -> dict:
    return {
        "b_id": existing.id,
        "b_locator_path": item["locator_path"],
        "b_locator_mtime_ns": item["locator_mtime_ns"],
        "b_last_seen_at": seen_at,
        "b_is_present": True,
    }


def _archive_size_conflict_message(
    summary: str,
    locator_key: str,
    first_size: int,
    first_path: str,
    observed_size: int,
    observed_path: str,
) -> str:
    return (
        f"{summary}: {locator_key!r}. "
        f"first_size={first_size} first_path={first_path}; "
        f"observed_size={observed_size} observed_path={observed_path}. "
        "A single archive locator must have one immutable size; remove the "
        "conflicting copy or narrow the scan root."
    )


def _quote_mssql_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def _mssql_table_name(table: sa.Table) -> str:
    if table.schema:
        return f"{_quote_mssql_identifier(table.schema)}.{_quote_mssql_identifier(table.name)}"
    return _quote_mssql_identifier(table.name)


def _rowcount(result) -> int:
    rowcount = result.rowcount
    return rowcount if rowcount is not None and rowcount >= 0 else 0


def _stage_rows(
    conn: sa.Connection,
    stmt,
    rows: list[dict],
    *,
    batch_size: int = MSSQL_ARCHIVE_STAGING_BATCH_SIZE,
    label: str | None = None,
) -> None:
    if not rows:
        return

    if label:
        logger.info(
            "%s: staging %s rows in %s-row batches.",
            label,
            len(rows),
            batch_size,
        )

    staged_count = 0
    for batch in _iter_batches(rows, batch_size):
        conn.execute(stmt, batch)
        staged_count += len(batch)
        if label:
            logger.info("%s: staged %s/%s rows.", label, staged_count, len(rows))


def _create_mssql_archive_staging_tables(conn: sa.Connection) -> None:
    for table_name in (
        "#archive_discovery",
        "#archive_discovery_one",
    ):
        conn.exec_driver_sql(
            f"IF OBJECT_ID('tempdb..{table_name}') IS NOT NULL DROP TABLE {table_name}"
        )

    conn.exec_driver_sql(
        """
        CREATE TABLE #archive_discovery (
            locator_key varchar(500) NOT NULL,
            locator_path varchar(500) NOT NULL,
            archive_key varchar(128) NOT NULL,
            buno varchar(6) NOT NULL,
            archive_datetime datetime2(0) NOT NULL,
            locator_size_bytes bigint NOT NULL,
            locator_mtime_ns bigint NOT NULL
        )
        """
    )
    conn.exec_driver_sql(
        """
        CREATE TABLE #archive_discovery_one (
            locator_key varchar(500) NOT NULL,
            locator_path varchar(500) NOT NULL,
            archive_key varchar(128) NOT NULL,
            buno varchar(6) NOT NULL,
            archive_datetime datetime2(0) NOT NULL,
            locator_size_bytes bigint NOT NULL,
            locator_mtime_ns bigint NOT NULL
        )
        """
    )


def _check_mssql_staged_archive_conflicts(conn: sa.Connection) -> None:
    conflict_key = conn.execute(
        sa.text(
            """
            SELECT TOP (1) locator_key
            FROM #archive_discovery
            GROUP BY locator_key
            HAVING COUNT(DISTINCT locator_size_bytes) > 1
            ORDER BY locator_key
            """
        )
    ).scalar()
    if conflict_key is None:
        return

    rows = conn.execute(
        sa.text(
            """
            SELECT TOP (2)
                locator_key,
                locator_size_bytes,
                MIN(locator_path) AS locator_path
            FROM #archive_discovery
            WHERE locator_key = :locator_key
            GROUP BY locator_key, locator_size_bytes
            ORDER BY locator_size_bytes
            """
        ),
        {"locator_key": conflict_key},
    ).fetchall()
    first = rows[0]
    observed = rows[1]
    raise ValueError(
        _archive_size_conflict_message(
            "Archive locator observed with different sizes in one scan",
            conflict_key,
            first.locator_size_bytes,
            first.locator_path,
            observed.locator_size_bytes,
            observed.locator_path,
        )
    )


def _check_mssql_existing_archive_size_conflicts(
    conn: sa.Connection, archive_table: str
) -> None:
    row = conn.execute(
        sa.text(
            f"""
            SELECT TOP (1)
                archive.locator_key,
                archive.locator_size_bytes AS first_size,
                archive.locator_path AS first_path,
                discovered.locator_size_bytes AS observed_size,
                discovered.locator_path AS observed_path
            FROM {archive_table} AS archive
            INNER JOIN #archive_discovery_one AS discovered
                ON discovered.locator_key = archive.locator_key
            WHERE archive.locator_size_bytes <> discovered.locator_size_bytes
            ORDER BY archive.locator_key
            """
        )
    ).first()
    if row is None:
        return

    raise ValueError(
        _archive_size_conflict_message(
            "Immutable archive locator changed size",
            row.locator_key,
            row.first_size,
            row.first_path,
            row.observed_size,
            row.observed_path,
        )
    )


def _dedupe_mssql_staged_archives(conn: sa.Connection) -> int:
    return _rowcount(
        conn.execute(
            sa.text(
                """
                INSERT INTO #archive_discovery_one (
                    locator_key,
                    locator_path,
                    archive_key,
                    buno,
                    archive_datetime,
                    locator_size_bytes,
                    locator_mtime_ns
                )
                SELECT
                    locator_key,
                    MIN(locator_path),
                    MIN(archive_key),
                    MIN(buno),
                    MIN(archive_datetime),
                    MIN(locator_size_bytes),
                    MAX(locator_mtime_ns)
                FROM #archive_discovery
                GROUP BY locator_key
                """
            )
        )
    )


def _mssql_table_has_rows(conn: sa.Connection, table_name: str) -> bool:
    return (
        conn.execute(sa.text(f"SELECT TOP (1) 1 FROM {table_name}")).first() is not None
    )


def _register_archives_bulk_mssql_staged(
    conn: sa.Connection,
    parsed_items: list[dict],
    seen_at: datetime,
) -> tuple[dict[Path, int], dict[str, int]]:
    timings: dict[str, float] = {}
    started = time.perf_counter()
    archive_table = _mssql_table_name(ArchiveMetadata.__table__)

    logger.info("Archive register: creating MSSQL staging tables.")
    _create_mssql_archive_staging_tables(conn)
    timings["create_staging"] = time.perf_counter() - started

    started = time.perf_counter()
    logger.info(
        "Archive register: staging locators=%s.",
        len(parsed_items),
    )
    if parsed_items:
        _stage_rows(
            conn,
            sa.text(
                """
                INSERT INTO #archive_discovery (
                    locator_key,
                    locator_path,
                    archive_key,
                    buno,
                    archive_datetime,
                    locator_size_bytes,
                    locator_mtime_ns
                )
                VALUES (
                    :locator_key,
                    :locator_path,
                    :archive_key,
                    :buno,
                    :archive_datetime,
                    :locator_size_bytes,
                    :locator_mtime_ns
                )
                """
            ),
            [
                {
                    "locator_key": item["locator_key"],
                    "locator_path": item["locator_path"],
                    "archive_key": item["archive_key"],
                    "buno": item["buno"],
                    "archive_datetime": item["archive_datetime"],
                    "locator_size_bytes": item["locator_size_bytes"],
                    "locator_mtime_ns": item["locator_mtime_ns"],
                }
                for item in parsed_items
            ],
            label="Archive register locators",
        )
    timings["stage_rows"] = time.perf_counter() - started

    started = time.perf_counter()
    logger.info("Archive register: checking locator conflicts.")
    _check_mssql_staged_archive_conflicts(conn)
    deduped_count = _dedupe_mssql_staged_archives(conn)
    logger.info(
        "Archive register: unique locators=%s.",
        deduped_count,
    )
    _check_mssql_existing_archive_size_conflicts(conn, archive_table)
    timings["conflict_and_dedupe"] = time.perf_counter() - started

    started = time.perf_counter()
    logger.info("Archive register: updating existing locators.")
    updated_count = _rowcount(
        conn.execute(
            sa.text(
                f"""
                UPDATE archive
                SET
                    locator_path = discovered.locator_path,
                    locator_mtime_ns = discovered.locator_mtime_ns,
                    last_seen_at = :seen_at,
                    is_present = 1
                FROM {archive_table} AS archive
                INNER JOIN #archive_discovery_one AS discovered
                    ON discovered.locator_key = archive.locator_key
                WHERE
                    archive.locator_path <> discovered.locator_path
                    OR archive.locator_mtime_ns <> discovered.locator_mtime_ns
                    OR archive.is_present = 0
                """
            ),
            {"seen_at": seen_at},
        )
    )
    logger.info(
        "Archive register: updated locators=%s.",
        updated_count,
    )
    timings["update_existing"] = time.perf_counter() - started

    started = time.perf_counter()
    logger.info("Archive register: inserting new locators.")
    if _mssql_table_has_rows(conn, archive_table):
        inserted_count = _rowcount(
            conn.execute(
                sa.text(
                    f"""
                INSERT INTO {archive_table} (
                    archive_key,
                    buno,
                    archive_datetime,
                    locator_key,
                    locator_path,
                    locator_size_bytes,
                    locator_mtime_ns,
                    first_seen_at,
                    last_seen_at,
                    is_present,
                    catalog_version
                )
                SELECT
                    discovered.archive_key,
                    discovered.buno,
                    discovered.archive_datetime,
                    discovered.locator_key,
                    discovered.locator_path,
                    discovered.locator_size_bytes,
                    discovered.locator_mtime_ns,
                    :seen_at,
                    :seen_at,
                    1,
                    0
                FROM #archive_discovery_one AS discovered
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {archive_table} AS archive WITH (UPDLOCK, HOLDLOCK)
                    WHERE archive.locator_key = discovered.locator_key
                )
                """
                ),
                {"seen_at": seen_at},
            )
        )
    else:
        logger.info("Archive register: archive table is empty; inserting directly.")
        inserted_count = _rowcount(
            conn.execute(
                sa.text(
                    f"""
                INSERT INTO {archive_table} (
                    archive_key,
                    buno,
                    archive_datetime,
                    locator_key,
                    locator_path,
                    locator_size_bytes,
                    locator_mtime_ns,
                    first_seen_at,
                    last_seen_at,
                    is_present,
                    catalog_version
                )
                SELECT
                    discovered.archive_key,
                    discovered.buno,
                    discovered.archive_datetime,
                    discovered.locator_key,
                    discovered.locator_path,
                    discovered.locator_size_bytes,
                    discovered.locator_mtime_ns,
                    :seen_at,
                    :seen_at,
                    1,
                    0
                FROM #archive_discovery_one AS discovered
                """
                ),
                {"seen_at": seen_at},
            )
        )
    logger.info(
        "Archive register: inserted locators=%s.",
        inserted_count,
    )
    timings["insert_missing"] = time.perf_counter() - started

    started = time.perf_counter()
    logger.info("Archive register: reconciling absent locators.")
    absent_count = _rowcount(
        conn.execute(
            sa.text(
                f"""
                UPDATE archive
                SET is_present = 0
                FROM {archive_table} AS archive
                LEFT JOIN #archive_discovery_one AS discovered
                    ON discovered.locator_key = archive.locator_key
                WHERE archive.is_present = 1
                    AND discovered.locator_key IS NULL
                """
            )
        )
    )
    logger.info(
        "Archive register: absent locators=%s.",
        absent_count,
    )
    timings["mark_absent"] = time.perf_counter() - started

    started = time.perf_counter()
    logger.info("Archive register: loading archive ids.")
    archive_id_map = {
        row.locator_key: row.id
        for row in conn.execute(
            sa.text(
                f"""
                SELECT archive.locator_key, archive.id
                FROM {archive_table} AS archive
                INNER JOIN #archive_discovery_one AS discovered
                    ON discovered.locator_key = archive.locator_key
                """
            )
        )
    }
    logger.info(
        "Archive register: archive ids=%s.",
        len(archive_id_map),
    )
    timings["load_ids"] = time.perf_counter() - started

    logger.info(
        "Archive register timings: create_staging=%.2fs "
        "stage_rows=%.2fs conflict_and_dedupe=%.2fs update_existing=%.2fs "
        "insert_missing=%.2fs mark_absent=%.2fs load_ids=%.2fs",
        timings["create_staging"],
        timings["stage_rows"],
        timings["conflict_and_dedupe"],
        timings["update_existing"],
        timings["insert_missing"],
        timings["mark_absent"],
        timings["load_ids"],
    )

    result_map: dict[Path, int] = {}
    for item in parsed_items:
        archive_id = archive_id_map.get(_archive_locator_key(item))
        if archive_id is not None:
            result_map[item["obj_path"]] = archive_id

    stats = {
        "deduped": deduped_count,
        "inserted": inserted_count,
        "updated": updated_count,
        "existing_skipped": max(deduped_count - inserted_count - updated_count, 0),
        "absent": absent_count,
    }
    return result_map, stats


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
                    # A concurrent refresh may have inserted this locator_path after the
                    # prefetch step. Treat that as a benign race and let the final id
                    # reload pick up the existing row.
                    continue

        inserted_count += len(batch)
        logger.info("Inserted %s/%s new archive rows...", inserted_count, len(rows))


def _is_mssql_deadlock(exc: Exception) -> bool:
    message = str(exc).lower()
    return "deadlock victim" in message or "(1205)" in message


def _has_present_archives(conn: sa.Connection) -> bool:
    return (
        conn.execute(
            sa.select(ArchiveMetadata.id)
            .where(ArchiveMetadata.is_present == sa.true())
            .limit(1)
        ).first()
        is not None
    )


def _mark_absent_archives(
    conn: sa.Connection,
    seen_locator_keys: set[str],
) -> int:
    absent_ids: list[int] = []
    rows = conn.execute(
        sa.select(
            ArchiveMetadata.id,
            ArchiveMetadata.locator_key,
        ).where(ArchiveMetadata.is_present == sa.true())
    )
    for row in rows:
        if row.locator_key not in seen_locator_keys:
            absent_ids.append(row.id)

    for i in range(0, len(absent_ids), ARCHIVE_LOOKUP_BATCH_SIZE):
        conn.execute(
            sa.update(ArchiveMetadata)
            .where(
                ArchiveMetadata.id.in_(absent_ids[i : i + ARCHIVE_LOOKUP_BATCH_SIZE])
            )
            .values(
                is_present=False,
            )
        )

    return len(absent_ids)


def register_archives_bulk(
    eng: sa.Engine,
    discovered_archives: List[DiscoveredArchive | Path],
) -> Dict[Path, int]:
    """
    Record the current archive locator scan in the canonical archive inventory.

    Returns a map of {Path: archive_id} for all discovered and parseable archives.
    """
    for attempt in range(1, ARCHIVE_REGISTRATION_DEADLOCK_RETRIES + 1):
        try:
            return _register_archives_bulk_once(
                eng,
                discovered_archives,
            )
        except DBAPIError as exc:
            if attempt < ARCHIVE_REGISTRATION_DEADLOCK_RETRIES and _is_mssql_deadlock(
                exc
            ):
                delay = ARCHIVE_REGISTRATION_DEADLOCK_DELAY_SECONDS * attempt
                logger.warning(
                    "Deadlock registering archives; retrying in %.1fs (%s/%s).",
                    delay,
                    attempt,
                    ARCHIVE_REGISTRATION_DEADLOCK_RETRIES - 1,
                )
                time.sleep(delay)
                continue
            raise

    raise RuntimeError("Archive registration retry loop exited unexpectedly")


def _register_archives_bulk_once(
    eng: sa.Engine,
    discovered_archives: List[DiscoveredArchive | Path],
) -> Dict[Path, int]:
    parsed_items = []
    for record in discovered_archives:
        parsed = _parse_archive_record(record)
        if parsed is not None:
            parsed_items.append(parsed)

    unique_locator_keys = list(
        dict.fromkeys(_archive_locator_key(item) for item in parsed_items)
    )
    seen_at = datetime.now(UTC).replace(tzinfo=None)
    seen_locator_keys = set(unique_locator_keys)

    if eng.dialect.name == "mssql":
        with eng.begin() as conn:
            if not parsed_items:
                if _has_present_archives(conn):
                    raise ValueError(
                        "Archive scan found no parseable locators; refusing to mark "
                        "existing archives absent."
                    )
                return {}
            result_map, stats = _register_archives_bulk_mssql_staged(
                conn,
                parsed_items,
                seen_at,
            )
        logger.info(
            "Archive registration complete. discovered_locators=%s deduped=%s "
            "inserted=%s existing_updated=%s existing_skipped=%s absent=%s",
            len(unique_locator_keys),
            stats["deduped"],
            stats["inserted"],
            stats["updated"],
            stats["existing_skipped"],
            stats["absent"],
        )
        return result_map

    with eng.begin() as conn:
        if not parsed_items:
            if _has_present_archives(conn):
                raise ValueError(
                    "Archive scan found no parseable locators; refusing to mark "
                    "existing archives absent."
                )
            return {}

        existing_map = _load_existing_archive_rows(conn, unique_locator_keys)

        to_insert: list[dict] = []
        to_update: list[dict] = []
        seen_in_batch: dict[str, dict] = {}
        updated_existing_count = 0
        skipped_existing_count = 0

        for item in parsed_items:
            locator_key = _archive_locator_key(item)
            seen_item = seen_in_batch.get(locator_key)
            if seen_item is not None:
                if seen_item["locator_size_bytes"] != item["locator_size_bytes"]:
                    raise ValueError(
                        _archive_size_conflict_message(
                            "Archive locator observed with different sizes in one scan",
                            locator_key,
                            seen_item["locator_size_bytes"],
                            seen_item["locator_path"],
                            item["locator_size_bytes"],
                            item["locator_path"],
                        )
                    )
                continue
            seen_in_batch[locator_key] = item

            existing = existing_map.get(locator_key)
            if existing is None:
                to_insert.append(_build_archive_insert(item, seen_at))
                continue
            if existing.locator_size_bytes != item["locator_size_bytes"]:
                raise ValueError(
                    _archive_size_conflict_message(
                        "Immutable archive locator changed size",
                        locator_key,
                        existing.locator_size_bytes,
                        existing.locator_path,
                        item["locator_size_bytes"],
                        item["locator_path"],
                    )
                )

            update_row = _build_archive_update(item, existing, seen_at)
            update_needed = (
                existing.locator_path != update_row["b_locator_path"]
                or existing.locator_mtime_ns != update_row["b_locator_mtime_ns"]
                or not existing.is_present
            )
            if not update_needed:
                skipped_existing_count += 1
                continue

            updated_existing_count += 1
            to_update.append(update_row)

        if to_insert:
            logger.info("Inserting %s new archive locators.", len(to_insert))
            _insert_new_archives(conn, to_insert)

        if to_update:
            stmt = (
                sa.update(ArchiveMetadata)
                .where(ArchiveMetadata.id == bindparam("b_id"))
                .values(
                    locator_path=bindparam("b_locator_path"),
                    locator_mtime_ns=bindparam("b_locator_mtime_ns"),
                    last_seen_at=bindparam("b_last_seen_at"),
                    is_present=bindparam("b_is_present"),
                )
            )
            conn.execute(stmt, to_update)

        logger.info("Reconciling archive presence.")
        absent_count = _mark_absent_archives(
            conn,
            seen_locator_keys,
        )

        logger.info(
            "Loading IDs for %s discovered archive locators...",
            len(unique_locator_keys),
        )
        archive_id_map = _load_archive_id_map(conn, unique_locator_keys)

    logger.info(
        "Archive registration complete. discovered_locators=%s inserted=%s "
        "existing_updated=%s existing_skipped=%s absent=%s",
        len(unique_locator_keys),
        len(to_insert),
        updated_existing_count,
        skipped_existing_count,
        absent_count,
    )

    result_map: Dict[Path, int] = {}
    for item in parsed_items:
        archive_id = archive_id_map.get(_archive_locator_key(item))
        if archive_id is not None:
            result_map[item["obj_path"]] = archive_id

    return result_map
