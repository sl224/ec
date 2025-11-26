import logging
from pathlib import Path
from typing import Dict, List, Tuple

import sqlalchemy as sa
from sqlalchemy import bindparam, insert, select, update
from sqlalchemy.exc import IntegrityError

from e2ude_core.services.catalog import FileScanResult, catalog_staged_folder
from e2ude_core.db.models import FileHashRegistry, FileMetadata
from e2ude_core.orchestration.managers import JobManager

logger = logging.getLogger(__name__)

SCANNER_PIPELINE_ID = "MetadataScanHandler"
SCANNER_VERSION = 1


def run_metadata_scan(
    eng: sa.Engine,
    folder_id: int,
    target_path: Path,
    job_updater: JobManager,
):
    """
    Cataloging Phase.
    Scans the staged directory and updates DB metadata.
    """
    job_updater.mark_running("Scanning staged structure...")

    if not target_path.is_dir():
        logger.error(f"Scanner expects a directory: {target_path}")
        return

    raw_files: List[FileScanResult] = catalog_staged_folder(target_path)

    if not raw_files:
        logger.warning(f"Target {target_path} appears empty.")
        job_updater._rows_uploaded_in_scope = 0
        return

    job_updater.mark_running(f"Cataloging {len(raw_files)} files...")

    _upsert_metadata(eng, folder_id, raw_files)

    job_updater._rows_uploaded_in_scope = len(raw_files)
    logger.info(f"[{SCANNER_PIPELINE_ID}] Cataloged {len(raw_files)} files.")


def fetch_existing_files_map(eng: sa.Engine, folder_id: int):
    with eng.connect() as conn:
        query = select(
            FileMetadata.id,
            FileMetadata.hash_id,
            FileMetadata.file_type,
            FileMetadata.relative_path,
        ).where(FileMetadata.folder_id == folder_id)
        return [row._mapping for row in conn.execute(query)]


def _upsert_metadata(eng: sa.Engine, folder_id: int, files: List[FileScanResult]):
    try:
        with eng.begin() as conn:
            unique_md5s = list({f.md5 for f in files if f.md5})
            hash_map = _ensure_hashes_exist(conn, unique_md5s)
            to_insert, to_update = _detect_changes(conn, folder_id, files, hash_map)
            _commit_changes(conn, to_insert, to_update)
            # REMOVED: Update to FolderMetadata.scan_version

    except Exception:
        logger.error("Failed to upsert metadata.", exc_info=True)
        raise


def _ensure_hashes_exist(conn, unique_md5s: List[bytes]) -> Dict[bytes, int]:
    if not unique_md5s:
        return {}

    existing_rows = conn.execute(
        select(FileHashRegistry.md5).where(FileHashRegistry.md5.in_(unique_md5s))
    ).fetchall()
    existing_hashes = {row.md5 for row in existing_rows}
    missing_hashes = [h for h in unique_md5s if h not in existing_hashes]

    if missing_hashes:
        try:
            conn.execute(insert(FileHashRegistry), [{"md5": h} for h in missing_hashes])
        except IntegrityError:
            for h in missing_hashes:
                try:
                    conn.execute(insert(FileHashRegistry).values(md5=h))
                except IntegrityError:
                    pass

    id_rows = conn.execute(
        select(FileHashRegistry.id, FileHashRegistry.md5).where(
            FileHashRegistry.md5.in_(unique_md5s)
        )
    ).fetchall()
    return {row.md5: row.id for row in id_rows}


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
                        "b_file_type": f.file_type,
                        "b_file_size": f.file_size_bytes,
                    }
                )
        else:
            to_insert.append(
                {
                    "folder_id": folder_id,
                    "relative_path": path,
                    "hash_id": new_hid,
                    "file_type": f.file_type,
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