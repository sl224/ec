import logging
import time
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import sqlalchemy as sa
from sqlalchemy import bindparam, insert, select, update
from sqlalchemy.exc import IntegrityError

from e2ude_core.db.base_session import DEFAULT_SCHEMA
from e2ude_core.runtime_files import PipelineId
from e2ude_core.services.file_catalog import FileScanResult, catalog_staged_folder
from e2ude_core.db.models import FileHashRegistry, FileMetadata
from e2ude_core.orchestration.spec import JobRunResult

logger = logging.getLogger(__name__)

SCANNER_PIPELINE_ID = PipelineId("MetadataScanHandler")
SCANNER_VERSION = 1
MAX_METADATA_DEADLOCK_RETRIES = 4
METADATA_DEADLOCK_RETRY_DELAY_SECONDS = 0.2


def _is_mssql_deadlock(exc: Exception) -> bool:
    message = str(exc).lower()
    return "deadlock victim" in message or "(1205)" in message


def run_metadata_scan(
    eng: sa.Engine,
    folder_id: int,
    target_path: Path,
    report_progress: Callable[[str], None],
) -> JobRunResult:
    """
    Cataloging Phase.
    Scans the staged directory and updates DB metadata.
    """
    report_progress("Scanning staged structure...")

    if not target_path.is_dir():
        logger.error(f"Scanner expects a directory: {target_path}")
        return JobRunResult(
            rows_uploaded=0,
            completion_message="Scanner skipped because the staged directory was missing.",
        )

    raw_files: List[FileScanResult] = catalog_staged_folder(target_path)

    if not raw_files:
        logger.warning(f"Target {target_path} appears empty.")
        return JobRunResult(
            rows_uploaded=0,
            completion_message="Metadata scan completed with no staged files.",
        )

    report_progress(f"Cataloging {len(raw_files)} files...")

    _upsert_metadata(eng, folder_id, raw_files)

    logger.info("[%s] Cataloged %s files.", SCANNER_PIPELINE_ID, len(raw_files))
    return JobRunResult(
        rows_uploaded=len(raw_files),
        completion_message=f"Cataloged {len(raw_files)} files.",
    )


def _upsert_metadata(eng: sa.Engine, folder_id: int, files: List[FileScanResult]):
    for attempt in range(1, MAX_METADATA_DEADLOCK_RETRIES + 1):
        try:
            with eng.begin() as conn:
                unique_md5s = sorted({f.md5 for f in files if f.md5})
                hash_map = _ensure_hashes_exist(conn, unique_md5s)
                to_insert, to_update = _detect_changes(conn, folder_id, files, hash_map)
                _commit_changes(conn, to_insert, to_update)
            return
        except Exception as exc:
            if attempt < MAX_METADATA_DEADLOCK_RETRIES and _is_mssql_deadlock(exc):
                delay = METADATA_DEADLOCK_RETRY_DELAY_SECONDS * attempt
                logger.warning(
                    "Deadlock upserting metadata for folder %s; retrying in %.1fs (%s/%s).",
                    folder_id,
                    delay,
                    attempt,
                    MAX_METADATA_DEADLOCK_RETRIES - 1,
                )
                time.sleep(delay)
                continue

            logger.error("Failed to upsert metadata.", exc_info=True)
            raise


def _ensure_hashes_exist(conn, unique_md5s: List[bytes]) -> Dict[bytes, int]:
    if not unique_md5s:
        return {}

    return {md5: _get_or_create_hash_id(conn, md5) for md5 in unique_md5s}


def _get_or_create_hash_id(conn, md5: bytes) -> int:
    select_stmt = select(FileHashRegistry.id).where(FileHashRegistry.md5 == md5)

    if conn.dialect.name == "mssql":
        # Serialize competing workers on one hash key at a time to avoid
        # deadlocks from multi-key IN(...) scans inside concurrent transactions.
        row = conn.execute(
            sa.text(
                f"SELECT id FROM [{DEFAULT_SCHEMA}].[metadata_hash_registry] "
                "WITH (UPDLOCK, HOLDLOCK) WHERE md5 = :md5"
            ),
            {"md5": md5},
        ).first()
    else:
        row = conn.execute(select_stmt).first()

    if row is not None:
        return row[0]

    try:
        conn.execute(insert(FileHashRegistry).values(md5=md5))
    except IntegrityError:
        pass

    row = conn.execute(select_stmt).first()
    if row is None:
        raise RuntimeError("Unable to resolve hash registry id after insert.")

    return row[0]


def _detect_changes(
    conn, folder_id, files: List[FileScanResult], hash_map
) -> Tuple[List, List]:
    current_state = conn.execute(
        select(FileMetadata.id, FileMetadata.relative_path, FileMetadata.hash_id).where(
            FileMetadata.folder_id == folder_id
        )
    ).fetchall()

    existing_map = {row.relative_path: (row.id, row.hash_id) for row in current_state}

    to_insert = []
    to_update = []

    for f in files:
        if not f.md5:
            continue

        path = f.relative_path
        new_hid = hash_map.get(f.md5)

        if not new_hid:
            continue

        if path in existing_map:
            db_id, db_hid = existing_map[path]
            if db_hid != new_hid:
                to_update.append(
                    {
                        "b_id": db_id,
                        "b_hash_id": new_hid,
                        "b_file_type": f.file_type.value,
                        "b_file_size": f.file_size_bytes,
                    }
                )
        else:
            to_insert.append(
                {
                    "folder_id": folder_id,
                    "relative_path": path,
                    "hash_id": new_hid,
                    "file_type": f.file_type.value,
                    "file_size_bytes": f.file_size_bytes,
                }
            )
    return to_insert, to_update


def _commit_changes(conn, to_insert, to_update):
    if to_insert:
        conn.execute(insert(FileMetadata), to_insert)
    if to_update:
        stmt = (
            update(FileMetadata)
            .where(FileMetadata.id == bindparam("b_id"))
            .values(
                hash_id=bindparam("b_hash_id"),
                file_type=bindparam("b_file_type"),
                file_size_bytes=bindparam("b_file_size"),
            )
        )
        conn.execute(stmt, to_update)
