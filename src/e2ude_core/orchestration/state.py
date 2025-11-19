import logging
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

# Import models used for queries
from e2ude_core.db.models import (
    ProcessingSession,
    ProcessingJob,
    StatusEnum,
    FileMetadata,
)
from e2ude_core.pipelines.scanner import MetadataScanHandler
from e2ude_core.registry import HANDLER_REGISTRY

logger = logging.getLogger(__name__)


# --- Data Structures ---


class FolderState(Enum):
    UP_TO_DATE = auto()
    PARTIAL = auto()


@dataclass
class WorkDelta:
    status: FolderState
    missing_items: List[Tuple[int, str]] = field(default_factory=list)


# --- State Calculation Function ---


def get_folder_work_delta(
    eng: sa.Engine, folder_id: int, scan_version: int = 1
) -> Optional[WorkDelta]:
    """
    Determines the processing state of a folder by comparing the
    'Actual' completed jobs against the 'Expected' registry requirements.

    Args:
        eng: Database engine.
        folder_id: The folder to check.
        scan_version: The required version of the MetadataScanHandler.
                      If the last successful scan was older than this,
                      returns None (forcing a re-scan).

    Returns:
        None if the folder has never been successfully scanned (or scan is outdated).
        WorkDelta if scan is valid (status UP_TO_DATE or PARTIAL).
    """
    with eng.connect() as conn:
        # 1. Check if the folder has ever been successfully scanned
        #    AND if that scan meets the current version requirement.
        scan_job_row = conn.execute(
            sa.select(ProcessingJob.handler_version)
            .join(ProcessingSession, ProcessingJob.session_id == ProcessingSession.id)
            .where(
                ProcessingSession.folder_id == folder_id,
                ProcessingJob.pipeline_id == MetadataScanHandler.PIPELINE_ID,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
            .order_by(ProcessingJob.handler_version.desc())  # Get best version
            .limit(1)
        ).fetchone()

        if not scan_job_row:
            # Never scanned
            return None

        last_scan_version = scan_job_row[0]
        if last_scan_version < scan_version:
            logger.info(
                f"Folder {folder_id} scan outdated (v{last_scan_version} < v{scan_version}). Re-scanning."
            )
            return None

        # 2. Get ACTUAL state (all completed jobs)
        # Note: We might want to check versions here too, but typically
        # semantic invalidation happens at the job_scope level.
        # Here we just check "Is it done?".
        actual_stmt = (
            sa.select(ProcessingJob.hash_id, ProcessingJob.target_name)
            .join(FileMetadata, ProcessingJob.file_id == FileMetadata.id)
            .where(
                FileMetadata.folder_id == folder_id,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
        )
        actual_work = set(conn.execute(actual_stmt).fetchall())

        # 3. Get EXPECTED state (all files found * registry definitions)
        expected_work = set()
        files = conn.execute(
            sa.select(FileMetadata.hash_id, FileMetadata.file_type).where(
                FileMetadata.folder_id == folder_id
            )
        ).fetchall()

        for hash_id, file_type in files:
            handler = HANDLER_REGISTRY.get(file_type)
            if handler:
                for model in handler.expected_models:
                    expected_work.add((hash_id, model.__tablename__))

        # 4. Calculate the delta
        missing_items = list(expected_work - actual_work)

        if not missing_items:
            # If `files` was empty, both sets are empty, this is correct.
            return WorkDelta(status=FolderState.UP_TO_DATE)

        return WorkDelta(status=FolderState.PARTIAL, missing_items=missing_items)
