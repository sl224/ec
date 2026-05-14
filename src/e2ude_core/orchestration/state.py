from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Type

import sqlalchemy as sa

from e2ude_core.db.base_session import Base
from e2ude_core.db.models import (
    ArchiveMetadata,
    ArtifactManifest,
    FileMetadata,
    ProcessingJob,
    StatusEnum,
)
from e2ude_core.runtime_files import (
    HANDLED_FILE_SPECS,
    FileType,
    RuntimeFileSpec,
    coerce_file_type,
)

SQL_BATCH_SIZE = 2000


@dataclass(frozen=True)
class PendingArtifact:
    archive_id: int
    source_path: Path
    file_id: int
    relative_path: str
    hash_id: int
    file_type: FileType
    parser_id: str
    target_table: str
    parser_version: int
    target_model: Type[Base]


@dataclass(frozen=True)
class ParserWorkItem:
    archive_id: int
    source_path: Path
    file_id: int
    hash_id: int
    file_type: FileType
    relative_path: str
    parser_id: str
    parser_version: int
    spec: RuntimeFileSpec
    target_models: tuple[Type[Base], ...]


@dataclass(frozen=True)
class ArchiveRunPlan:
    needs_scan: bool
    work_items: tuple[ParserWorkItem, ...] = ()


def _parser_id(spec: RuntimeFileSpec) -> str:
    if spec.pipeline_id is None:
        raise ValueError("handled parser spec must have a pipeline_id")
    return spec.pipeline_id.value


def _handled_specs_by_parser_id() -> dict[str, RuntimeFileSpec]:
    return {_parser_id(spec): spec for spec in HANDLED_FILE_SPECS}


def archive_scan_current(row) -> bool:
    return (
        bool(row.is_present) and row.completed_scan_version >= row.required_scan_version
    )


def archive_needs_scan(row) -> bool:
    return (
        bool(row.is_present) and row.completed_scan_version < row.required_scan_version
    )


def archive_id_needs_scan(eng: sa.Engine, archive_id: int) -> bool:
    with eng.connect() as conn:
        row = conn.execute(
            sa.select(
                ArchiveMetadata.is_present,
                ArchiveMetadata.required_scan_version,
                ArchiveMetadata.completed_scan_version,
            ).where(ArchiveMetadata.id == archive_id)
        ).first()
    return True if row is None else archive_needs_scan(row)


def load_archives_requiring_scan(eng: sa.Engine) -> dict[Path, int]:
    with eng.connect() as conn:
        rows = conn.execute(
            sa.select(ArchiveMetadata.id, ArchiveMetadata.source_path).where(
                ArchiveMetadata.is_present == sa.true(),
                ArchiveMetadata.completed_scan_version
                < ArchiveMetadata.required_scan_version,
            )
        ).fetchall()

    return {Path(row.source_path): row.id for row in rows}


def _fetch_manifest_versions(
    conn: sa.Connection, hash_ids: Iterable[int]
) -> dict[int, dict[str, int]]:
    hash_id_list = sorted(set(hash_ids))
    if not hash_id_list:
        return {}

    versions: dict[int, dict[str, int]] = defaultdict(dict)
    for i in range(0, len(hash_id_list), SQL_BATCH_SIZE):
        batch = hash_id_list[i : i + SQL_BATCH_SIZE]
        rows = conn.execute(
            sa.select(
                ArtifactManifest.hash_id,
                ArtifactManifest.target_table,
                ArtifactManifest.parser_version,
            ).where(ArtifactManifest.hash_id.in_(batch))
        )
        for row in rows:
            current = versions[row.hash_id].get(row.target_table, 0)
            versions[row.hash_id][row.target_table] = max(current, row.parser_version)
    return versions


def _failed_artifact_keys(
    conn: sa.Connection, parser_ids: set[str]
) -> set[tuple[int, str, str]]:
    stmt = sa.select(
        ProcessingJob.hash_id,
        ProcessingJob.parser_id,
        ProcessingJob.target_table,
    ).where(
        ProcessingJob.status == StatusEnum.ERROR,
        ProcessingJob.hash_id.is_not(None),
        ProcessingJob.parser_id.in_(parser_ids),
        ProcessingJob.target_table.is_not(None),
    )
    return {
        (row.hash_id, row.parser_id, row.target_table) for row in conn.execute(stmt)
    }


def load_pending_artifacts(
    eng: sa.Engine,
    *,
    parser_id: str | None = None,
    failed_only: bool = False,
    archive_ids: list[int] | None = None,
    hash_ids: list[int] | None = None,
    limit: int | None = None,
    force: bool = False,
) -> list[PendingArtifact]:
    specs_by_parser = _handled_specs_by_parser_id()
    if parser_id is not None:
        if parser_id not in specs_by_parser:
            raise ValueError(f"Unknown parser {parser_id!r}")
        specs = (specs_by_parser[parser_id],)
    else:
        specs = tuple(specs_by_parser.values())

    file_types = {spec.file_type.value for spec in specs}
    spec_by_file_type = {spec.file_type: spec for spec in specs}

    with eng.connect() as conn:
        stmt = (
            sa.select(
                ArchiveMetadata.id.label("archive_id"),
                ArchiveMetadata.source_path,
                FileMetadata.id.label("file_id"),
                FileMetadata.relative_path,
                FileMetadata.hash_id,
                FileMetadata.file_type,
            )
            .select_from(FileMetadata)
            .join(ArchiveMetadata, ArchiveMetadata.id == FileMetadata.archive_id)
            .where(
                ArchiveMetadata.is_present == sa.true(),
                ArchiveMetadata.completed_scan_version
                >= ArchiveMetadata.required_scan_version,
                FileMetadata.file_type.in_(file_types),
            )
        )
        if archive_ids:
            stmt = stmt.where(FileMetadata.archive_id.in_(archive_ids))
        if hash_ids:
            stmt = stmt.where(FileMetadata.hash_id.in_(hash_ids))

        rows = conn.execute(stmt).fetchall()
        manifest_versions = _fetch_manifest_versions(
            conn, (row.hash_id for row in rows)
        )
        failed_keys = (
            _failed_artifact_keys(conn, {_parser_id(spec) for spec in specs})
            if failed_only
            else set()
        )

    representatives = {}
    for row in sorted(
        rows, key=lambda item: (item.hash_id, str(item.source_path), item.file_id)
    ):
        file_type = coerce_file_type(row.file_type)
        spec = spec_by_file_type.get(file_type)
        if spec is None:
            continue
        representatives.setdefault((row.hash_id, _parser_id(spec)), (row, spec))

    pending: list[PendingArtifact] = []
    for row, spec in representatives.values():
        current_versions = manifest_versions.get(row.hash_id, {})
        spec_parser_id = _parser_id(spec)
        for model in spec.expected_models:
            target_table = model.__tablename__
            if (
                failed_only
                and (
                    row.hash_id,
                    spec_parser_id,
                    target_table,
                )
                not in failed_keys
            ):
                continue
            if not force and current_versions.get(target_table, 0) >= (
                spec.version or 0
            ):
                continue
            pending.append(
                PendingArtifact(
                    archive_id=row.archive_id,
                    source_path=Path(row.source_path),
                    file_id=row.file_id,
                    relative_path=row.relative_path,
                    hash_id=row.hash_id,
                    file_type=spec.file_type,
                    parser_id=spec_parser_id,
                    target_table=target_table,
                    parser_version=spec.version or 0,
                    target_model=model,
                )
            )

    pending.sort(
        key=lambda item: (
            str(item.source_path),
            item.relative_path,
            item.hash_id,
            item.parser_id,
            item.target_table,
        )
    )
    if limit is not None:
        allowed_groups: set[tuple[int, str]] = set()
        limited: list[PendingArtifact] = []
        for item in pending:
            key = (item.hash_id, item.parser_id)
            if key not in allowed_groups:
                if len(allowed_groups) >= limit:
                    continue
                allowed_groups.add(key)
            limited.append(item)
        pending = limited
    return pending


def group_pending_artifacts(
    artifacts: Iterable[PendingArtifact],
) -> tuple[ParserWorkItem, ...]:
    specs_by_parser = _handled_specs_by_parser_id()
    grouped: dict[tuple[int, str], list[PendingArtifact]] = defaultdict(list)
    for artifact in artifacts:
        grouped[(artifact.hash_id, artifact.parser_id)].append(artifact)

    items: list[ParserWorkItem] = []
    for (_hash_id, parser_id), group in grouped.items():
        group = sorted(group, key=lambda item: item.target_table)
        first = group[0]
        items.append(
            ParserWorkItem(
                archive_id=first.archive_id,
                source_path=first.source_path,
                file_id=first.file_id,
                hash_id=first.hash_id,
                file_type=first.file_type,
                relative_path=first.relative_path,
                parser_id=parser_id,
                parser_version=first.parser_version,
                spec=specs_by_parser[parser_id],
                target_models=tuple(item.target_model for item in group),
            )
        )

    return tuple(
        sorted(
            items,
            key=lambda item: (
                str(item.source_path),
                item.relative_path,
                item.hash_id,
                item.parser_id,
            ),
        )
    )


def count_parser_artifacts(
    eng: sa.Engine, specs: Iterable[RuntimeFileSpec]
) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for spec in specs:
        artifacts = load_pending_artifacts(eng, parser_id=_parser_id(spec))
        with eng.connect() as conn:
            file_rows = conn.execute(
                sa.select(FileMetadata.hash_id)
                .join(ArchiveMetadata, ArchiveMetadata.id == FileMetadata.archive_id)
                .where(
                    ArchiveMetadata.is_present == sa.true(),
                    ArchiveMetadata.completed_scan_version
                    >= ArchiveMetadata.required_scan_version,
                    FileMetadata.file_type == spec.file_type.value,
                )
            ).fetchall()
            hash_ids = {row.hash_id for row in file_rows}
            target_tables = [model.__tablename__ for model in spec.expected_models]
            rows_uploaded = 0
            complete = 0
            if hash_ids and target_tables:
                manifest_rows = conn.execute(
                    sa.select(
                        ArtifactManifest.hash_id,
                        ArtifactManifest.target_table,
                        ArtifactManifest.row_count,
                    ).where(
                        ArtifactManifest.hash_id.in_(hash_ids),
                        ArtifactManifest.target_table.in_(target_tables),
                        ArtifactManifest.parser_version >= (spec.version or 0),
                    )
                ).fetchall()
                by_hash: dict[int, set[str]] = defaultdict(set)
                for row in manifest_rows:
                    by_hash[row.hash_id].add(row.target_table)
                    rows_uploaded += row.row_count or 0
                complete = sum(
                    1
                    for table_set in by_hash.values()
                    if len(table_set) == len(target_tables)
                )

        counts[_parser_id(spec)] = {
            "files": len(file_rows),
            "hashes": len(hash_ids),
            "complete": complete,
            "missing": len({(item.hash_id, item.parser_id) for item in artifacts}),
            "rows": rows_uploaded,
        }
    return counts


def plan_archive_run(eng: sa.Engine, archive_id: int) -> ArchiveRunPlan:
    needs_scan = archive_id_needs_scan(eng, archive_id)
    if needs_scan:
        return ArchiveRunPlan(needs_scan=True)

    return ArchiveRunPlan(
        needs_scan=False,
        work_items=group_pending_artifacts(
            load_pending_artifacts(eng, archive_ids=[archive_id])
        ),
    )


def load_archives_requiring_work(eng: sa.Engine) -> dict[Path, int]:
    work = load_archives_requiring_scan(eng)
    for item in load_pending_artifacts(eng):
        work.setdefault(item.source_path, item.archive_id)
    return work
