import logging
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Tuple

import sqlalchemy as sa
from sqlalchemy import bindparam, insert, select, update
from sqlalchemy.exc import IntegrityError

from e2ude_core.services.zip_io import RecursiveZipScanner
from e2ude_core.db.models import FileMetadata, FileHashRegistry
from e2ude_core.services.zip_io import file_type_patterns
from e2ude_core.orchestration.managers import JobManager
from e2ude_core.pipelines.base import BaseHandler

logger = logging.getLogger(__name__)


class FileToProcess(NamedTuple):
    file_id: int
    hash_id: int
    file_type: str
    relative_path: str
    full_path: Path


class MetadataScanHandler(BaseHandler):
    PIPELINE_ID = "MetadataScanHandler"
    VERSION = 1
    # Explicitly define BOTH output tables this handler populates
    expected_models = [FileMetadata, FileHashRegistry]

    def __init__(self, eng: sa.Engine, folder_id: int, extract_dir: Path):
        self.eng = eng
        self.folder_id = folder_id
        self.extract_dir = extract_dir
        self.pattern_map = {v: k for k, v in file_type_patterns.items()}

    def run(
        self,
        eng: sa.Engine,
        hash_id: int,  # Unused for scanner (can be None/0)
        file_path: Path,  # This is the Zip Path
        job_updater: JobManager,
        keys_to_process: List[str] = None,  # Unused
    ):
        """
        Implements BaseHandler.run contract.
        """
        job_updater.mark_running("Scanning zip archive structure...")

        scanner = RecursiveZipScanner(file_path)
        raw_files = scanner.scan()

        if not raw_files:
            logger.warning(f"Zip file {file_path} appears empty or unreadable.")
            job_updater._rows_uploaded_in_scope = 0
            return

        job_updater.mark_running(f"Cataloging {len(raw_files)} files...")

        self.upsert(raw_files)

        # Report Total Files Found as the "Rows" metric for the Scan Job
        job_updater._rows_uploaded_in_scope = len(raw_files)
        logger.info(f"[{self.PIPELINE_ID}] Cataloged {len(raw_files)} files.")

    def upsert(self, files: List[Dict[str, Any]]):
        try:
            with self.eng.begin() as conn:
                unique_md5s = list({f["md5"] for f in files})
                hash_map = self._ensure_hashes_exist(conn, unique_md5s)
                to_insert, to_update = self._detect_changes(conn, files, hash_map)
                self._commit_changes(conn, to_insert, to_update)
        except Exception:
            logger.error("Failed to upsert metadata.", exc_info=True)
            raise

    def fetch_existing_files(self) -> List[FileToProcess]:
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

    def _ensure_hashes_exist(self, conn, unique_md5s: List[bytes]) -> Dict[str, int]:
        existing_rows = conn.execute(
            select(FileHashRegistry.md5).where(FileHashRegistry.md5.in_(unique_md5s))
        ).fetchall()
        existing_hashes = {row.md5 for row in existing_rows}
        missing_hashes = [h for h in unique_md5s if h not in existing_hashes]

        if missing_hashes:
            try:
                conn.execute(
                    insert(FileHashRegistry), [{"md5": h} for h in missing_hashes]
                )
            except IntegrityError:
                logger.warning(
                    "Hash collision in batch. Switching to iterative insert."
                )
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

    def _detect_changes(self, conn, files, hash_map) -> Tuple[List, List]:
        current_state = conn.execute(
            select(
                FileMetadata.id, FileMetadata.relative_path, FileMetadata.hash_id
            ).where(FileMetadata.folder_id == self.folder_id)
        ).fetchall()

        existing_map = {
            row.relative_path: (row.id, row.hash_id) for row in current_state
        }

        to_insert = []
        to_update = []

        for f in files:
            path = f["relative_path"]
            new_hid = hash_map.get(f["md5"])
            if not new_hid:
                continue

            if path in existing_map:
                db_id, db_hid = existing_map[path]
                if db_hid != new_hid:
                    to_update.append(
                        {
                            "b_id": db_id,
                            "b_hash_id": new_hid,
                            "b_file_type": f["file_type"],
                            "b_file_size": f["file_size_bytes"],
                        }
                    )
            else:
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
