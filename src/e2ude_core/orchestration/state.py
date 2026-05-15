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
)
from e2ude_core.runtime_files import (
    CURRENT_ARCHIVE_CATALOG_VERSION,
    HANDLED_FILE_SPECS,
    FileType,
    RuntimeFileSpec,
    artifact_key_for,
    handled_specs_for_path,
    parser_id_for,
)

SQL_BATCH_SIZE = 2000


@dataclass(frozen=True)
class PendingArtifact:
    archive_id: int
    locator_path: Path
    file_id: int
    relative_path: str
    content_hash: bytes | None
    file_type: FileType
    parser_id: str
    artifact_key: str
    target_table: str
    parser_version: int
    target_model: Type[Base]


@dataclass(frozen=True)
class ParserWorkItem:
    archive_id: int
    locator_path: Path
    file_id: int
    content_hash: bytes | None
    file_type: FileType
    relative_path: str
    parser_id: str
    parser_version: int
    spec: RuntimeFileSpec
    target_models: tuple[Type[Base], ...]


@dataclass(frozen=True)
class ArchiveRunPlan:
    needs_catalog: bool
    work_items: tuple[ParserWorkItem, ...] = ()


def _parser_id(spec: RuntimeFileSpec) -> str:
    return parser_id_for(spec)


def _handled_specs_by_parser_id() -> dict[str, RuntimeFileSpec]:
    return {_parser_id(spec): spec for spec in HANDLED_FILE_SPECS}


def archive_catalog_current(row) -> bool:
    return (
        bool(row.is_present)
        and row.cataloged_at is not None
        and row.catalog_version >= CURRENT_ARCHIVE_CATALOG_VERSION
    )


def archive_needs_catalog(row) -> bool:
    return bool(row.is_present) and not archive_catalog_current(row)


def archive_id_needs_catalog(eng: sa.Engine, archive_id: int) -> bool:
    with eng.connect() as conn:
        row = conn.execute(
            sa.select(
                ArchiveMetadata.is_present,
                ArchiveMetadata.cataloged_at,
                ArchiveMetadata.catalog_version,
            ).where(ArchiveMetadata.id == archive_id)
        ).first()
    return True if row is None else archive_needs_catalog(row)


def load_archives_requiring_catalog(eng: sa.Engine) -> dict[Path, int]:
    with eng.connect() as conn:
        rows = conn.execute(
            sa.select(ArchiveMetadata.id, ArchiveMetadata.locator_path).where(
                ArchiveMetadata.is_present == sa.true(),
                sa.or_(
                    ArchiveMetadata.cataloged_at.is_(None),
                    ArchiveMetadata.catalog_version < CURRENT_ARCHIVE_CATALOG_VERSION,
                ),
            )
        ).fetchall()

    return {Path(row.locator_path): row.id for row in rows}


def _fetch_manifest_states(
    conn: sa.Connection, content_hashes: Iterable[bytes]
) -> dict[bytes, dict[str, tuple[int, str]]]:
    content_hash_list = sorted(set(content_hashes))
    if not content_hash_list:
        return {}

    states: dict[bytes, dict[str, tuple[int, str]]] = defaultdict(dict)
    for i in range(0, len(content_hash_list), SQL_BATCH_SIZE):
        batch = content_hash_list[i : i + SQL_BATCH_SIZE]
        rows = conn.execute(
            sa.select(
                ArtifactManifest.content_hash,
                ArtifactManifest.artifact_key,
                ArtifactManifest.target_table,
                ArtifactManifest.parser_version,
            ).where(ArtifactManifest.content_hash.in_(batch))
        )
        for row in rows:
            current = states[row.content_hash].get(row.artifact_key)
            if current is None or row.parser_version > current[0]:
                states[row.content_hash][row.artifact_key] = (
                    row.parser_version,
                    row.target_table,
                )
    return states


def _select_catalog_rows(
    conn: sa.Connection,
    *,
    archive_ids: list[int] | None,
    content_hashes: list[bytes] | None,
):
    stmt = (
        sa.select(
            ArchiveMetadata.id.label("archive_id"),
            ArchiveMetadata.locator_path,
            FileMetadata.id.label("file_id"),
            FileMetadata.relative_path,
            FileMetadata.content_hash,
        )
        .select_from(FileMetadata)
        .join(ArchiveMetadata, ArchiveMetadata.id == FileMetadata.archive_id)
        .where(
            ArchiveMetadata.is_present == sa.true(),
            ArchiveMetadata.cataloged_at.is_not(None),
            ArchiveMetadata.catalog_version >= CURRENT_ARCHIVE_CATALOG_VERSION,
        )
    )
    if archive_ids:
        stmt = stmt.where(FileMetadata.archive_id.in_(archive_ids))
    if content_hashes:
        stmt = stmt.where(FileMetadata.content_hash.in_(content_hashes))
    return conn.execute(stmt).fetchall()


def load_pending_artifacts(
    eng: sa.Engine,
    *,
    parser_id: str | None = None,
    archive_ids: list[int] | None = None,
    content_hashes: list[bytes] | None = None,
    limit: int | None = None,
    force: bool = False,
) -> list[PendingArtifact]:
    specs_by_parser = _handled_specs_by_parser_id()
    if parser_id is not None:
        if parser_id not in specs_by_parser:
            raise ValueError(f"Unknown parser {parser_id!r}")
        allowed_specs = (specs_by_parser[parser_id],)
    else:
        allowed_specs = tuple(specs_by_parser.values())
    allowed_by_id = {_parser_id(spec): spec for spec in allowed_specs}

    with eng.connect() as conn:
        rows = _select_catalog_rows(
            conn,
            archive_ids=archive_ids,
            content_hashes=content_hashes,
        )
        manifest_states = _fetch_manifest_states(
            conn, (row.content_hash for row in rows if row.content_hash is not None)
        )

    pending: list[PendingArtifact] = []
    for row in sorted(
        rows,
        key=lambda item: (
            item.content_hash or b"",
            str(item.locator_path),
            item.file_id,
        ),
    ):
        for spec in handled_specs_for_path(row.relative_path):
            spec_parser_id = _parser_id(spec)
            if spec_parser_id not in allowed_by_id:
                continue

            current_states = manifest_states.get(row.content_hash, {})
            for model in spec.expected_models:
                artifact_key = artifact_key_for(spec, model)
                target_table = model.__tablename__
                current_state = current_states.get(artifact_key)
                if (
                    row.content_hash is not None
                    and not force
                    and current_state is not None
                    and current_state[0] >= (spec.version or 0)
                    and current_state[1] == target_table
                ):
                    continue
                pending.append(
                    PendingArtifact(
                        archive_id=row.archive_id,
                        locator_path=Path(row.locator_path),
                        file_id=row.file_id,
                        relative_path=row.relative_path,
                        content_hash=row.content_hash,
                        file_type=spec.file_type,
                        parser_id=spec_parser_id,
                        artifact_key=artifact_key,
                        target_table=target_table,
                        parser_version=spec.version or 0,
                        target_model=model,
                    )
                )

    pending.sort(
        key=lambda item: (
            str(item.locator_path),
            item.relative_path,
            item.content_hash or b"",
            item.parser_id,
            item.artifact_key,
            item.target_table,
        )
    )
    if limit is not None:
        allowed_groups: set[tuple[bytes | None, int, str]] = set()
        limited: list[PendingArtifact] = []
        for item in pending:
            key = (item.content_hash, item.file_id, item.parser_id)
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
    grouped: dict[tuple[bytes | None, int, str], list[PendingArtifact]] = defaultdict(
        list
    )
    for artifact in artifacts:
        grouped[(artifact.content_hash, artifact.file_id, artifact.parser_id)].append(
            artifact
        )

    items: list[ParserWorkItem] = []
    for (_content_hash, _file_id, parser_id), group in grouped.items():
        group = sorted(group, key=lambda item: item.target_table)
        first = group[0]
        items.append(
            ParserWorkItem(
                archive_id=first.archive_id,
                locator_path=first.locator_path,
                file_id=first.file_id,
                content_hash=first.content_hash,
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
                str(item.locator_path),
                item.relative_path,
                item.content_hash or b"",
                item.parser_id,
            ),
        )
    )


def count_parser_artifacts(
    eng: sa.Engine, specs: Iterable[RuntimeFileSpec]
) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for spec in specs:
        parser_id = _parser_id(spec)
        artifacts = load_pending_artifacts(eng, parser_id=parser_id)
        with eng.connect() as conn:
            file_rows = [
                row
                for row in _select_catalog_rows(
                    conn,
                    archive_ids=None,
                    content_hashes=None,
                )
                if spec in handled_specs_for_path(row.relative_path)
            ]
            content_hashes = {
                row.content_hash for row in file_rows if row.content_hash is not None
            }
            artifact_targets = {
                artifact_key_for(spec, model): model.__tablename__
                for model in spec.expected_models
            }
            rows_uploaded = 0
            complete = 0
            if content_hashes and artifact_targets:
                manifest_rows = conn.execute(
                    sa.select(
                        ArtifactManifest.content_hash,
                        ArtifactManifest.artifact_key,
                        ArtifactManifest.target_table,
                        ArtifactManifest.row_count,
                    ).where(
                        ArtifactManifest.content_hash.in_(content_hashes),
                        ArtifactManifest.artifact_key.in_(list(artifact_targets)),
                        ArtifactManifest.parser_version >= (spec.version or 0),
                    )
                ).fetchall()
                by_hash: dict[bytes, set[str]] = defaultdict(set)
                for row in manifest_rows:
                    if artifact_targets.get(row.artifact_key) != row.target_table:
                        continue
                    by_hash[row.content_hash].add(row.artifact_key)
                    rows_uploaded += row.row_count or 0
                complete = sum(
                    1
                    for artifact_set in by_hash.values()
                    if len(artifact_set) == len(artifact_targets)
                )

        counts[parser_id] = {
            "files": len(file_rows),
            "hashed": sum(1 for row in file_rows if row.content_hash is not None),
            "hashes": len(content_hashes),
            "complete": complete,
            "missing": len(
                {
                    (item.content_hash, item.file_id, item.parser_id)
                    for item in artifacts
                }
            ),
            "rows": rows_uploaded,
        }
    return counts


def target_models_needing_work(
    eng: sa.Engine,
    *,
    content_hash: bytes,
    spec: RuntimeFileSpec,
    target_models: tuple[Type[Base], ...],
    force: bool = False,
) -> tuple[Type[Base], ...]:
    if force:
        return target_models

    with eng.connect() as conn:
        states = _fetch_manifest_states(conn, [content_hash]).get(content_hash, {})

    return tuple(
        model
        for model in target_models
        if (
            (state := states.get(artifact_key_for(spec, model))) is None
            or state[0] < (spec.version or 0)
            or state[1] != model.__tablename__
        )
    )


def plan_archive_run(eng: sa.Engine, archive_id: int) -> ArchiveRunPlan:
    needs_catalog = archive_id_needs_catalog(eng, archive_id)
    if needs_catalog:
        return ArchiveRunPlan(needs_catalog=True)

    return ArchiveRunPlan(
        needs_catalog=False,
        work_items=group_pending_artifacts(
            load_pending_artifacts(eng, archive_ids=[archive_id])
        ),
    )


def load_archives_requiring_work(eng: sa.Engine) -> dict[Path, int]:
    work = load_archives_requiring_catalog(eng)
    for item in load_pending_artifacts(eng):
        work.setdefault(item.locator_path, item.archive_id)
    return work
