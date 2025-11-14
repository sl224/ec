import logging
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

# Import models used for queries
from etude_core.db.models import (
    ProcessingSession,
    ProcessingJob,
    StatusEnum,
    FileMetadata,
)
from etude_core.pipelines.scanner import MetadataScanHandler
from etude_core.registry import HANDLER_REGISTRY

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


def get_folder_work_delta(eng: sa.Engine, folder_id: int) -> Optional[WorkDelta]:
    """
    Determines the processing state of a folder by comparing the
    'Actual' completed jobs against the 'Expected' registry requirements.

    Returns None if the folder has never been successfully scanned.
    """
    with eng.connect() as conn:
        # 1. Check if the folder has ever been successfully scanned
        scan_complete = conn.execute(
            sa.select(ProcessingJob.id)
            .join(ProcessingSession, ProcessingJob.session_id == ProcessingSession.id)
            .where(
                ProcessingSession.folder_id == folder_id,
                ProcessingJob.pipeline_id == MetadataScanHandler.PIPELINE_ID,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
            .limit(1)
        ).scalar()

        if not scan_complete:
            # This is a NEW folder (or scan failed). Needs to be scanned.
            return None

        # 2. Get ACTUAL state (all completed jobs)
        actual_stmt = (
            sa.select(ProcessingJob.hash_id, ProcessingJob.dataset_key)
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
