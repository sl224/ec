from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Tuple, Type

import sqlalchemy as sa

from e2ude_core.db.base_session import Base
from e2ude_core.db.models import (
    ArchiveMetadata,
    ArchiveStateEnum,
    ArtifactManifest,
    FileMetadata,
)
from e2ude_core.registry import CURRENT_HANDLER_GENERATION, HANDLER_REGISTRY
from e2ude_core.runtime_files import FileType, PipelineId, coerce_file_type

logger = logging.getLogger(__name__)

SQL_BATCH_SIZE = 2000


@dataclass(frozen=True)
class ArchiveSummary:
    status: ArchiveStateEnum
    work_reason: str | None = None


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
class ArchiveRunPlan:
    summary: ArchiveSummary
    work_items: Tuple[FileWorkPlan, ...] = ()


def summarize_archive_facts(
    *,
    is_present: bool,
    completed_scan_version: int,
    required_scan_version: int,
    completed_handler_generation: str | None,
    required_handler_generation: str,
    stored_state: ArchiveStateEnum,
    work_reason: str | None,
) -> ArchiveSummary:
    if not is_present:
        return ArchiveSummary(
            status=ArchiveStateEnum.UP_TO_DATE,
            work_reason="Archive not present on source share",
        )

    if completed_scan_version < required_scan_version:
        return ArchiveSummary(
            status=ArchiveStateEnum.NEEDS_SCAN,
            work_reason=work_reason or "Archive scan required",
        )

    if completed_handler_generation != required_handler_generation:
        return ArchiveSummary(
            status=ArchiveStateEnum.NEEDS_PROCESSING,
            work_reason=work_reason or "Archive processing required",
        )

    if stored_state == ArchiveStateEnum.UP_TO_DATE:
        return ArchiveSummary(status=ArchiveStateEnum.UP_TO_DATE)

    return ArchiveSummary(
        status=ArchiveStateEnum.NEEDS_PROCESSING,
        work_reason=work_reason or "Archive processing required",
    )


def _fetch_archive_rows(
    conn: sa.Connection, archive_ids: Iterable[int]
) -> Dict[int, sa.Row]:
    archive_id_list = list(archive_ids)
    if not archive_id_list:
        return {}

    rows: Dict[int, sa.Row] = {}
    for i in range(0, len(archive_id_list), SQL_BATCH_SIZE):
        batch = archive_id_list[i : i + SQL_BATCH_SIZE]
        query = sa.select(
            ArchiveMetadata.id,
            ArchiveMetadata.state,
            ArchiveMetadata.work_reason,
            ArchiveMetadata.required_scan_version,
            ArchiveMetadata.completed_scan_version,
            ArchiveMetadata.required_handler_generation,
            ArchiveMetadata.completed_handler_generation,
            ArchiveMetadata.is_present,
        ).where(ArchiveMetadata.id.in_(batch))
        for row in conn.execute(query):
            rows[row.id] = row

    return rows


def _fetch_archive_files(
    conn: sa.Connection, archive_ids: Iterable[int]
) -> Dict[int, list]:
    archive_id_list = list(archive_ids)
    if not archive_id_list:
        return {}

    archive_files = defaultdict(list)
    for i in range(0, len(archive_id_list), SQL_BATCH_SIZE):
        batch = archive_id_list[i : i + SQL_BATCH_SIZE]
        query = sa.select(
            FileMetadata.archive_id,
            FileMetadata.id,
            FileMetadata.hash_id,
            FileMetadata.file_type,
            FileMetadata.relative_path,
        ).where(FileMetadata.archive_id.in_(batch))
        for row in conn.execute(query):
            archive_files[row.archive_id].append(row)

    return archive_files


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


def _summarize_archive_row(row: sa.Row | None) -> ArchiveSummary:
    if row is None:
        return ArchiveSummary(
            status=ArchiveStateEnum.NEEDS_SCAN,
            work_reason="Archive missing from inventory",
        )

    return summarize_archive_facts(
        is_present=row.is_present,
        completed_scan_version=row.completed_scan_version,
        required_scan_version=row.required_scan_version,
        completed_handler_generation=row.completed_handler_generation,
        required_handler_generation=row.required_handler_generation,
        stored_state=row.state,
        work_reason=row.work_reason,
    )


def summarize_archives_bulk(
    eng: sa.Engine,
    archive_ids: list[int],
) -> Dict[int, ArchiveSummary]:
    if not archive_ids:
        return {}

    with eng.connect() as conn:
        archive_rows = _fetch_archive_rows(conn, archive_ids)

    return {
        archive_id: _summarize_archive_row(archive_rows.get(archive_id))
        for archive_id in archive_ids
    }


def summarize_archive(eng: sa.Engine, archive_id: int) -> ArchiveSummary:
    return summarize_archives_bulk(eng, [archive_id]).get(
        archive_id,
        ArchiveSummary(
            status=ArchiveStateEnum.NEEDS_SCAN,
            work_reason="Archive missing from inventory",
        ),
    )


def plan_archive_run(eng: sa.Engine, archive_id: int) -> ArchiveRunPlan:
    summary = summarize_archive(eng, archive_id)
    if summary.status == ArchiveStateEnum.NEEDS_SCAN:
        return ArchiveRunPlan(summary=summary)

    with eng.connect() as conn:
        file_rows = _fetch_archive_files(conn, [archive_id]).get(archive_id, [])
        artifact_versions = _fetch_artifact_versions(
            conn, (row.hash_id for row in file_rows)
        )

    if not file_rows:
        return ArchiveRunPlan(
            summary=ArchiveSummary(status=ArchiveStateEnum.UP_TO_DATE),
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
        return ArchiveRunPlan(
            summary=ArchiveSummary(status=ArchiveStateEnum.UP_TO_DATE),
            work_items=(),
        )

    ordered_items = tuple(
        sorted(work_items, key=lambda item: (item.relative_path, item.file_type.value))
    )
    return ArchiveRunPlan(
        summary=ArchiveSummary(status=ArchiveStateEnum.NEEDS_PROCESSING),
        work_items=ordered_items,
    )


def load_archives_requiring_work(eng: sa.Engine) -> Dict[Path, int]:
    """Load all present archives whose canonical work state is not up to date."""
    with eng.connect() as conn:
        rows = conn.execute(
            sa.select(ArchiveMetadata.id, ArchiveMetadata.source_path).where(
                ArchiveMetadata.is_present == sa.true(),
                ArchiveMetadata.state != ArchiveStateEnum.UP_TO_DATE,
            )
        ).fetchall()

    return {Path(row.source_path): row.id for row in rows}


def mark_archive_scan_complete(
    eng: sa.Engine,
    archive_id: int,
    *,
    work_reason: str | None = None,
) -> None:
    with eng.begin() as conn:
        conn.execute(
            sa.update(ArchiveMetadata)
            .where(ArchiveMetadata.id == archive_id)
            .values(
                completed_scan_version=ArchiveMetadata.required_scan_version,
                state=ArchiveStateEnum.NEEDS_PROCESSING,
                work_reason=work_reason,
                last_error_at=None,
                last_error_message=None,
            )
        )


def mark_archive_processing_complete(eng: sa.Engine, archive_id: int) -> None:
    with eng.begin() as conn:
        conn.execute(
            sa.update(ArchiveMetadata)
            .where(ArchiveMetadata.id == archive_id)
            .values(
                completed_handler_generation=CURRENT_HANDLER_GENERATION,
                state=ArchiveStateEnum.UP_TO_DATE,
                work_reason=None,
                last_success_at=sa.func.now(),
                last_error_at=None,
                last_error_message=None,
            )
        )


def mark_archive_error(
    eng: sa.Engine,
    archive_id: int,
    *,
    state: ArchiveStateEnum,
    error_message: str,
) -> None:
    with eng.begin() as conn:
        conn.execute(
            sa.update(ArchiveMetadata)
            .where(ArchiveMetadata.id == archive_id)
            .values(
                state=state,
                last_error_at=sa.func.now(),
                last_error_message=error_message,
            )
        )
