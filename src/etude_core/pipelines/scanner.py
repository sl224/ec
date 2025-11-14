import logging
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Tuple

import sqlalchemy as sa
from sqlalchemy import bindparam, insert, select, update
from sqlalchemy.exc import IntegrityError

from etude_core.services.fs_scanner import scan_directory
from etude_core.db.models import FileMetadata, FileHashRegistry
from etude_core.services.zip_io import FileType, file_type_patterns
from etude_core.orchestration.managers import JobManager

logger = logging.getLogger(__name__)


# DTO for a file that needs to be processed.
class FileToProcess(NamedTuple):
    file_id: int
    hash_id: int
    file_type: str
    relative_path: str
    full_path: Path


class MetadataScanHandler:
    PIPELINE_ID = "MetadataScanHandler"

    def __init__(self, eng: sa.Engine, folder_id: int, extract_dir: Path):
        self.eng = eng
        self.folder_id = folder_id
        self.extract_dir = extract_dir
        self.pattern_map = {v: k for k, v in file_type_patterns.items()}

    def run(self, job_updater: JobManager, should_skip: bool) -> List[FileToProcess]:
        if should_skip:
            logger.info(
                f"[{self.PIPELINE_ID}] Job skipped. Fetching existing file list."
            )
            return self._fetch_existing_files()

        job_updater.mark_running("Scanning directory...")
        raw_files = scan_directory(self.extract_dir, self.pattern_map, FileType.UNKNOWN)

        if not raw_files:
            job_updater._rows_uploaded_in_scope = 0
            return []

        job_updater.mark_running(f"Cataloging {len(raw_files)} files...")
        self._upsert_metadata(raw_files)

        job_updater._rows_uploaded_in_scope = len(raw_files)
        return self._fetch_existing_files()

    def _upsert_metadata(self, files: List[Dict[str, Any]]):
        """
        Orchestrates DB metadata updates via a 3-step process:
        1. Resolve: Ensure all file hashes exist in the registry.
        2. Diff: Detect new or changed files.
        3. Execute: Commit inserts and updates to the database.
        """
        with self.eng.begin() as conn:
            unique_md5s = list({f["md5"] for f in files})
            hash_map = self._ensure_hashes_exist(conn, unique_md5s)

            to_insert, to_update = self._detect_changes(conn, files, hash_map)

            self._commit_changes(conn, to_insert, to_update)

    def _ensure_hashes_exist(self, conn, unique_md5s: List[str]) -> Dict[str, int]:
        """
        Ensures all MD5 hashes exist in the `metadata_hash_registry` table
        and returns a map of `{md5: id}`. Uses a bulk-then-iterative
        insert pattern to handle potential race conditions safely.
        """
        # A. Filter out hashes we already have
        existing_rows = conn.execute(
            select(FileHashRegistry.md5).where(FileHashRegistry.md5.in_(unique_md5s))
        ).fetchall()
        existing_hashes = {row.md5 for row in existing_rows}

        missing_hashes = [h for h in unique_md5s if h not in existing_hashes]

        # B. Insert missing hashes (Optimistic Bulk -> Pessimistic Loop)
        if missing_hashes:
            try:
                # Fast path: bulk insert all missing hashes.
                conn.execute(
                    insert(FileHashRegistry), [{"md5": h} for h in missing_hashes]
                )
            except IntegrityError:
                # Slow path: fallback to iterative inserts on collision.
                logger.warning(
                    "Hash collision in batch. Switching to iterative insert."
                )
                for h in missing_hashes:
                    try:
                        conn.execute(insert(FileHashRegistry).values(md5=h))
                    except IntegrityError:
                        pass  # Another process inserted it; this is safe.

        # C. Fetch all IDs, which are now guaranteed to exist.
        id_rows = conn.execute(
            select(FileHashRegistry.id, FileHashRegistry.md5).where(
                FileHashRegistry.md5.in_(unique_md5s)
            )
        ).fetchall()

        # After all inserts, the number of hashes in the DB must match our list
        assert len(id_rows) == len(
            unique_md5s
        ), "Hash resolution failed: count mismatch."

        return {row.md5: row.id for row in id_rows}

    def _detect_changes(self, conn, files, hash_map) -> Tuple[List, List]:
        """
        Compares scanned files against DB state to find new or modified files.
        """
        # Fetch current state for this folder
        current_state = conn.execute(
            select(
                FileMetadata.id, FileMetadata.relative_path, FileMetadata.hash_id
            ).where(FileMetadata.folder_id == self.folder_id)
        ).fetchall()

        # Map: path -> (db_id, db_hash_id)
        existing_map = {
            row.relative_path: (row.id, row.hash_id) for row in current_state
        }

        to_insert = []
        to_update = []

        for f in files:
            path = f["relative_path"]
            new_hid = hash_map.get(f["md5"])

            if not new_hid:
                logger.error(f"Logic Error: Missing ID for hash {f['md5']}")
                continue

            if path in existing_map:
                db_id, db_hid = existing_map[path]

                # If file content has changed, update the hash_id.
                if db_hid != new_hid:
                    logger.warning(
                        f"🚨 DATA MUTATION DETECTED 🚨\n"
                        f"    The file content on disk does not match the database record.\n"
                        f"    Folder ID:   {self.folder_id}\n"
                        f"    File Path:   {path}\n"
                        f"    Old HashID:  {db_hid}\n"
                        f"    New HashID:  {new_hid}\n"
                        f"    ACTION:      Updating database to match new source (Self-Healing)."
                    )

                    to_update.append(
                        {
                            "b_id": db_id,
                            "b_hash_id": new_hid,
                            "b_file_type": f["file_type"],
                            "b_file_size": f["file_size_bytes"],
                        }
                    )
            else:
                # New file
                to_insert.append(
                    {
                        "folder_id": self.folder_id,
                        "relative_path": path,
                        "hash_id": new_hid,
                        "file_type": f["file_type"],
                        "file_size_bytes": f["file_size_bytes"],
                    }
                )

        return to_insert, to_update

    def _commit_changes(self, conn, to_insert, to_update):
        """Executes bulk insert and update statements."""
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

    def _fetch_existing_files(self) -> List[FileToProcess]:
        """
        Queries the DB to get the list of all files associated with the folder.
        """
        with self.eng.connect() as conn:
            query = select(
                FileMetadata.id,
                FileMetadata.hash_id,
                FileMetadata.file_type,
                FileMetadata.relative_path,
            ).where(FileMetadata.folder_id == self.folder_id)

            results = []
            for row in conn.execute(query):
                results.append(
                    FileToProcess(
                        file_id=row.id,
                        hash_id=row.hash_id,
                        file_type=row.file_type,
                        relative_path=row.relative_path,
                        full_path=self.extract_dir / row.relative_path,
                    )
                )
            return results
