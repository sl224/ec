import logging
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

from e2ude_core.db.models import (
    FileMetadata,
    ArtifactManifest,
    ProcessingSession, 
    ProcessingJob,
    StatusEnum
)
from e2ude_core.registry import HANDLER_REGISTRY

logger = logging.getLogger(__name__)


class FolderState(Enum):
    UP_TO_DATE = auto()
    INCOMPLETE = auto()
    NEEDS_SCAN = auto()


@dataclass
class WorkDelta:
    status: FolderState
    missing_items: List[Tuple[int, str]] = field(default_factory=list)
    scan_reason: Optional[str] = None


def get_folder_work_delta(
    eng: sa.Engine, folder_id: int, scan_version: int = 1
) -> WorkDelta:
    with eng.connect() as conn:
        # 1. Check Scan Status via ProcessingJob History
        # Join Session -> Job to find if this folder has a completed scan job
        scan_stmt = (
            sa.select(sa.func.max(ProcessingJob.handler_version))
            .join(ProcessingSession, ProcessingJob.session_id == ProcessingSession.id)
            .where(
                ProcessingSession.folder_id == folder_id,
                ProcessingJob.pipeline_id == "MetadataScanHandler", 
                ProcessingJob.status == StatusEnum.COMPLETED
            )
        )
        
        current_scan_ver = conn.execute(scan_stmt).scalar()
        
        # If None, it means no scan has ever completed successfully for this folder
        if current_scan_ver is None:
            return WorkDelta(
                status=FolderState.NEEDS_SCAN, scan_reason="New Folder (No scan history)"
            )

        if current_scan_ver < scan_version:
            return WorkDelta(
                status=FolderState.NEEDS_SCAN,
                scan_reason=f"Outdated Scan (v{current_scan_ver} < v{scan_version})",
            )

        # 2. Get ACTUAL state (From ArtifactManifest)
        # What data do we actually have for files in this folder?
        actual_stmt = (
            sa.select(FileMetadata.hash_id, ArtifactManifest.target_table)
            .join(FileMetadata, FileMetadata.hash_id == ArtifactManifest.hash_id)
            .where(FileMetadata.folder_id == folder_id)
        )
        actual_rows = conn.execute(actual_stmt).fetchall()
        actual_work = {(row.hash_id, row.target_table) for row in actual_rows}

        # 3. Get EXPECTED state
        # What data SHOULD we have based on the file types present?
        expected_work = set()
        files = conn.execute(
            sa.select(FileMetadata.hash_id, FileMetadata.file_type).where(
                FileMetadata.folder_id == folder_id
            )
        ).fetchall()

        for hash_id, file_type in files:
            handler_spec = HANDLER_REGISTRY.get(file_type)
            if handler_spec:
                for model in handler_spec.expected_models:
                    expected_work.add((hash_id, model.__tablename__))

        # 4. Calculate Delta
        missing_items = list(expected_work - actual_work)

        if not missing_items:
            return WorkDelta(status=FolderState.UP_TO_DATE)

        return WorkDelta(status=FolderState.INCOMPLETE, missing_items=missing_items)