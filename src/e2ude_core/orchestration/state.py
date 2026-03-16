from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Type

import sqlalchemy as sa

from e2ude_core.db.base_session import Base
from e2ude_core.db.models import (
    ArtifactManifest,
    FileMetadata,
    ProcessingJob,
    ProcessingSession,
    StatusEnum,
)
from e2ude_core.pipelines.scanner import SCANNER_PIPELINE_ID
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.runtime_files import FileType, PipelineId, coerce_file_type

logger = logging.getLogger(__name__)


class FolderState(Enum):
    UP_TO_DATE = auto()
    INCOMPLETE = auto()
    NEEDS_SCAN = auto()


@dataclass(frozen=True)
class FolderSummary:
    status: FolderState
    scan_reason: Optional[str] = None


@dataclass(frozen=True)
class FileWorkPlan:
    file_id: int
    hash_id: int
    file_type: FileType
    relative_path: str
    target_models: Tuple[Type[Base], ...]
    pipeline_id: PipelineId
    handler_version: int


@dataclass(frozen=True)
class FolderRunPlan:
    summary: FolderSummary
    work_items: Tuple[FileWorkPlan, ...] = ()


SQL_BATCH_SIZE = 2000


def _fetch_completed_scan_versions(
    conn: sa.Connection, folder_ids: Iterable[int]
) -> Dict[int, int]:
    folder_id_list = list(folder_ids)
    if not folder_id_list:
        return {}

    versions: Dict[int, int] = {}
    for i in range(0, len(folder_id_list), SQL_BATCH_SIZE):
        batch = folder_id_list[i : i + SQL_BATCH_SIZE]
        query = (
            sa.select(
                ProcessingSession.folder_id,
                sa.func.max(ProcessingJob.handler_version).label("scan_version"),
            )
            .join(ProcessingJob, ProcessingJob.session_id == ProcessingSession.id)
            .where(
                ProcessingSession.folder_id.in_(batch),
                ProcessingJob.pipeline_id == SCANNER_PIPELINE_ID.value,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
            .group_by(ProcessingSession.folder_id)
        )
        for row in conn.execute(query):
            versions[row.folder_id] = row.scan_version

    return versions


def _fetch_folder_files(
    conn: sa.Connection, folder_ids: Iterable[int]
) -> Dict[int, list]:
    folder_id_list = list(folder_ids)
    if not folder_id_list:
        return {}

    folder_files = defaultdict(list)
    for i in range(0, len(folder_id_list), SQL_BATCH_SIZE):
        batch = folder_id_list[i : i + SQL_BATCH_SIZE]
        query = sa.select(
            FileMetadata.folder_id,
            FileMetadata.id,
            FileMetadata.hash_id,
            FileMetadata.file_type,
            FileMetadata.relative_path,
        ).where(FileMetadata.folder_id.in_(batch))
        for row in conn.execute(query):
            folder_files[row.folder_id].append(row)

    return folder_files


def _fetch_artifact_versions(
    conn: sa.Connection, hash_ids: Iterable[int]
) -> Dict[int, Dict[str, int]]:
    hash_id_list = sorted(set(hash_ids))
    if not hash_id_list:
        return {}

    artifacts: Dict[int, Dict[str, int]] = defaultdict(dict)
    for i in range(0, len(hash_id_list), SQL_BATCH_SIZE):
        batch = hash_id_list[i : i + SQL_BATCH_SIZE]
        query = sa.select(
            ArtifactManifest.hash_id,
            ArtifactManifest.target_table,
            ArtifactManifest.handler_version,
        ).where(ArtifactManifest.hash_id.in_(batch))
        for row in conn.execute(query):
            current = artifacts[row.hash_id].get(row.target_table, 0)
            artifacts[row.hash_id][row.target_table] = max(current, row.handler_version)

    return artifacts


def summarize_folders_bulk(
    eng: sa.Engine, folder_ids: List[int], scan_version: int
) -> Dict[int, FolderSummary]:
    if not folder_ids:
        return {}

    results = {
        folder_id: FolderSummary(status=FolderState.NEEDS_SCAN)
        for folder_id in folder_ids
    }

    with eng.connect() as conn:
        scan_versions = _fetch_completed_scan_versions(conn, folder_ids)

        ready_for_data_check = [
            folder_id
            for folder_id in folder_ids
            if scan_versions.get(folder_id, 0) >= scan_version
        ]

        if not ready_for_data_check:
            return results

        folder_files = _fetch_folder_files(conn, ready_for_data_check)
        artifacts = _fetch_artifact_versions(
            conn,
            (row.hash_id for rows in folder_files.values() for row in rows),
        )

    for folder_id in ready_for_data_check:
        files = folder_files.get(folder_id, [])
        if not files:
            results[folder_id] = FolderSummary(status=FolderState.UP_TO_DATE)
            continue

        seen_keys: set[tuple[int, FileType]] = set()
        status = FolderState.UP_TO_DATE
        for row in files:
            file_type = coerce_file_type(row.file_type)
            dedupe_key = (row.hash_id, file_type)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            handler_spec = HANDLER_REGISTRY.get(file_type)
            if not handler_spec:
                continue

            actual_tables = artifacts.get(row.hash_id, {})
            if any(
                actual_tables.get(model.__tablename__, 0) < handler_spec.version
                for model in handler_spec.expected_models
            ):
                status = FolderState.INCOMPLETE
                break

        results[folder_id] = FolderSummary(status=status)

    return results


def summarize_folder(
    eng: sa.Engine, folder_id: int, scan_version: int = 1
) -> FolderSummary:
    with eng.connect() as conn:
        current_scan_version = _fetch_completed_scan_versions(conn, [folder_id]).get(
            folder_id
        )

    if current_scan_version is None:
        return FolderSummary(
            status=FolderState.NEEDS_SCAN,
            scan_reason="New Folder",
        )

    if current_scan_version < scan_version:
        return FolderSummary(
            status=FolderState.NEEDS_SCAN,
            scan_reason="Outdated Scan",
        )

    return summarize_folders_bulk(eng, [folder_id], scan_version).get(
        folder_id, FolderSummary(status=FolderState.NEEDS_SCAN)
    )


def plan_folder_run(
    eng: sa.Engine, folder_id: int, scan_version: int = 1
) -> FolderRunPlan:
    summary = summarize_folder(eng, folder_id, scan_version)
    if summary.status == FolderState.NEEDS_SCAN:
        return FolderRunPlan(summary=summary)

    with eng.connect() as conn:
        file_rows = _fetch_folder_files(conn, [folder_id]).get(folder_id, [])
        artifact_versions = _fetch_artifact_versions(
            conn, (row.hash_id for row in file_rows)
        )

    if not file_rows:
        return FolderRunPlan(
            summary=FolderSummary(status=FolderState.UP_TO_DATE),
            work_items=(),
        )

    representatives: dict[tuple[int, FileType], tuple[object, FileType]] = {}
    for row in sorted(
        file_rows,
        key=lambda item: (
            item.hash_id,
            coerce_file_type(item.file_type).value,
            item.id,
        ),
    ):
        file_type = coerce_file_type(row.file_type)
        representatives.setdefault((row.hash_id, file_type), (row, file_type))

    work_items: list[FileWorkPlan] = []
    for row, file_type in representatives.values():
        handler_spec = HANDLER_REGISTRY.get(file_type)
        if not handler_spec:
            continue

        current_versions = artifact_versions.get(row.hash_id, {})
        missing_models = tuple(
            model
            for model in handler_spec.expected_models
            if current_versions.get(model.__tablename__, 0) < handler_spec.version
        )
        if not missing_models:
            continue

        work_items.append(
            FileWorkPlan(
                file_id=row.id,
                hash_id=row.hash_id,
                file_type=file_type,
                relative_path=row.relative_path,
                target_models=missing_models,
                pipeline_id=handler_spec.pipeline_id,
                handler_version=handler_spec.version,
            )
        )

    if not work_items:
        return FolderRunPlan(
            summary=FolderSummary(status=FolderState.UP_TO_DATE),
            work_items=(),
        )

    ordered_items = tuple(
        sorted(work_items, key=lambda item: (item.relative_path, item.file_type.value))
    )
    return FolderRunPlan(
        summary=FolderSummary(status=FolderState.INCOMPLETE),
        work_items=ordered_items,
    )


def select_folders_requiring_work(
    eng: sa.Engine,
    folder_map: Dict[Path, int],
    scan_version: int,
    *,
    batch_size: int = 5000,
    progress_callback: Callable[[int], None] | None = None,
) -> Dict[Path, int]:
    """Return the folder subset whose summaries are not UP_TO_DATE."""
    total = len(folder_map)
    logger.info("Checking state for %s folders (Bulk Mode)...", total)

    needed: Dict[Path, int] = {}
    all_paths = list(folder_map.keys())

    for i in range(0, total, batch_size):
        chunk_paths = all_paths[i : i + batch_size]
        chunk_ids = [folder_map[path] for path in chunk_paths]

        try:
            summaries = summarize_folders_bulk(eng, chunk_ids, scan_version)
            for path, folder_id in zip(chunk_paths, chunk_ids):
                summary = summaries.get(
                    folder_id, FolderSummary(status=FolderState.NEEDS_SCAN)
                )
                if summary.status != FolderState.UP_TO_DATE:
                    needed[path] = folder_id
        except Exception as exc:
            logger.error("Failed batch state check: %s", exc)
            for path, folder_id in zip(chunk_paths, chunk_ids):
                needed[path] = folder_id
        finally:
            if progress_callback is not None:
                progress_callback(len(chunk_paths))

    logger.info("State check complete. %s folders require processing.", len(needed))
    return needed
