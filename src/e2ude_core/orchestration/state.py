import logging
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Set
from collections import defaultdict

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
    """Single-folder check (Legacy/Fallback)."""
    # Re-use the bulk logic for consistency
    states = get_folder_states_bulk(eng, [folder_id], scan_version)
    state = states.get(folder_id, FolderState.NEEDS_SCAN)
    
    # If UP_TO_DATE, return immediately
    if state == FolderState.UP_TO_DATE:
        return WorkDelta(status=FolderState.UP_TO_DATE)
        
    # For the "Process" phase, we usually re-calculate delta to get the specific 'missing_items'.
    # The bulk function returns a lightweight Enum to save memory.
    # Below is the detailed check logic for when we actually process the folder.
    
    with eng.connect() as conn:
        # 1. Check Scan
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
        
        if current_scan_ver is None:
            return WorkDelta(status=FolderState.NEEDS_SCAN, scan_reason="New Folder")
        if current_scan_ver < scan_version:
            return WorkDelta(status=FolderState.NEEDS_SCAN, scan_reason="Outdated Scan")

        # 2. Actual
        actual_stmt = (
            sa.select(FileMetadata.hash_id, ArtifactManifest.target_table)
            .join(FileMetadata, FileMetadata.hash_id == ArtifactManifest.hash_id)
            .where(FileMetadata.folder_id == folder_id)
        )
        actual_work = {(row.hash_id, row.target_table) for row in conn.execute(actual_stmt)}

        # 3. Expected
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

        missing = list(expected_work - actual_work)
        if not missing:
            return WorkDelta(status=FolderState.UP_TO_DATE)
        
        return WorkDelta(status=FolderState.INCOMPLETE, missing_items=missing)


def get_folder_states_bulk(
    eng: sa.Engine, 
    folder_ids: List[int], 
    scan_version: int
) -> Dict[int, FolderState]:
    """
    Efficiently checks the state of multiple folders.
    Handles internal batching to respect SQL parameter limits.
    """
    if not folder_ids:
        return {}

    # Default assumption: Everything needs a scan
    results = {fid: FolderState.NEEDS_SCAN for fid in folder_ids}
    
    # Internal Chunking for SQL safety (MSSQL limit ~2100 params)
    # Increased from 1500 to 2000 to maximize throughput per query
    SQL_BATCH_SIZE = 2000
    
    scanned_folder_ids = set()
    
    with eng.connect() as conn:
        # 1. Check Scan Status (Batched)
        for i in range(0, len(folder_ids), SQL_BATCH_SIZE):
            batch = folder_ids[i : i + SQL_BATCH_SIZE]
            scan_query = (
                sa.select(ProcessingSession.folder_id)
                .join(ProcessingJob, ProcessingJob.session_id == ProcessingSession.id)
                .where(
                    ProcessingSession.folder_id.in_(batch),
                    ProcessingJob.pipeline_id == "MetadataScanHandler",
                    ProcessingJob.status == StatusEnum.COMPLETED,
                    ProcessingJob.handler_version >= scan_version
                )
                .group_by(ProcessingSession.folder_id)
            )
            scanned_folder_ids.update(conn.execute(scan_query).scalars().all())

        # Folders that haven't been scanned stay as NEEDS_SCAN.
        # We only inspect artifacts for the scanned ones.
        folders_to_check = list(scanned_folder_ids)
        if not folders_to_check:
            return results

        # 2. Fetch File Metadata (Batched)
        folder_files = defaultdict(list)
        all_hash_ids = set()
        
        for i in range(0, len(folders_to_check), SQL_BATCH_SIZE):
            batch = folders_to_check[i : i + SQL_BATCH_SIZE]
            file_query = (
                sa.select(FileMetadata.folder_id, FileMetadata.hash_id, FileMetadata.file_type)
                .where(FileMetadata.folder_id.in_(batch))
            )
            for row in conn.execute(file_query):
                folder_files[row.folder_id].append((row.hash_id, row.file_type))
                all_hash_ids.add(row.hash_id)

        # 3. Fetch Artifact Manifests (Batched)
        existing_artifacts = defaultdict(set)
        hash_id_list = list(all_hash_ids)
        
        for i in range(0, len(hash_id_list), SQL_BATCH_SIZE):
            batch = hash_id_list[i : i + SQL_BATCH_SIZE]
            art_query = (
                sa.select(ArtifactManifest.hash_id, ArtifactManifest.target_table)
                .where(ArtifactManifest.hash_id.in_(batch))
            )
            for row in conn.execute(art_query):
                existing_artifacts[row.hash_id].add(row.target_table)

    # 4. Compute Logic in Memory (Fast)
    for fid in folders_to_check:
        state = FolderState.UP_TO_DATE
        
        files = folder_files.get(fid, [])
        if not files:
            # Scanned but no files found inside? It's technically up-to-date.
            results[fid] = FolderState.UP_TO_DATE
            continue

        for hash_id, file_type in files:
            spec = HANDLER_REGISTRY.get(file_type)
            if not spec:
                continue 
            
            actual_tables = existing_artifacts.get(hash_id, set())
            
            missing_any = False
            for model in spec.expected_models:
                if model.__tablename__ not in actual_tables:
                    missing_any = True
                    break
            
            if missing_any:
                state = FolderState.INCOMPLETE
                break 
        
        results[fid] = state

    return results