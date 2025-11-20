import logging
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

from e2ude_core.db.models import (
    FileMetadata,
)

# CHANGED: Import the constant ID, not the removed class
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.db.models import ArtifactManifest, FolderMetadata

logger = logging.getLogger(__name__)


# --- Data Structures ---


class FolderState(Enum):
    UP_TO_DATE = auto()
    INCOMPLETE = auto()
    NEEDS_SCAN = auto()


@dataclass
class WorkDelta:
    status: FolderState
    # Tuple of (hash_id, dataset_key)
    missing_items: List[Tuple[int, str]] = field(default_factory=list)
    # Context for why a scan is needed
    scan_reason: Optional[str] = None


# --- State Calculation Function ---


# ... imports ...


def get_folder_work_delta(
    eng: sa.Engine, folder_id: int, scan_version: int = 1
) -> WorkDelta:
    with eng.connect() as conn:
        # 1. Check Scan Status via FolderMetadata
        current_scan_ver = conn.execute(
            sa.select(FolderMetadata.scan_version).where(FolderMetadata.id == folder_id)
        ).scalar_one_or_none()

        # Handle New Folder (current_scan_ver might be None if row missing, or 0 if default)
        if current_scan_ver is None:
            # Should technically not happen if process_zip ensures folder existence
            return WorkDelta(
                status=FolderState.NEEDS_SCAN, scan_reason="Folder not registered"
            )

        if current_scan_ver < scan_version:
            return WorkDelta(
                status=FolderState.NEEDS_SCAN,
                scan_reason=f"Outdated Scan (v{current_scan_ver} < v{scan_version})",
            )

        # 2. Get ACTUAL state (From Manifest, not Jobs)
        # Join Manifest -> FileMetadata to see what we have for *this* folder
        actual_stmt = (
            sa.select(FileMetadata.hash_id, ArtifactManifest.target_table)
            .join(FileMetadata, FileMetadata.hash_id == ArtifactManifest.hash_id)
            .where(
                FileMetadata.folder_id == folder_id
                # Implicitly, we accept ANY version in the manifest as "present",
                # but we should filter by version if we want to force upgrades.
            )
        )
        actual_rows = conn.execute(actual_stmt).fetchall()

        # If you want to enforce versioning per-file:
        # In the loop below, you'd check if `actual_version < required_version`

        actual_work = {(row.hash_id, row.target_table) for row in actual_rows}

        # 3. Get EXPECTED state (Same as before)
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
