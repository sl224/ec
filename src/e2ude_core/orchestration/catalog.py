from __future__ import annotations

from hashlib import md5, sha1
import logging
import time
from pathlib import Path
from typing import Callable

import sqlalchemy as sa
from sqlalchemy import insert, select, update

from e2ude_core.db.base_session import DEFAULT_SCHEMA
from e2ude_core.db.models import ArchiveMetadata, FileMetadata
from e2ude_core.orchestration.runs import JobRunResult
from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION
from e2ude_core.services.zip_io import ArchiveMember, iter_archive_members

logger = logging.getLogger(__name__)

CATALOG_PIPELINE_ID = "archive_catalog"
HASH_PIPELINE_ID = "content_hash"
MAX_CATALOG_DEADLOCK_RETRIES = 4
CATALOG_DEADLOCK_RETRY_DELAY_SECONDS = 0.2


def _is_mssql_deadlock(exc: Exception) -> bool:
    message = str(exc).lower()
    return "deadlock victim" in message or "(1205)" in message


def _lock_archive_row(conn: sa.Connection, archive_id: int) -> None:
    if conn.dialect.name == "mssql":
        conn.execute(
            sa.text(
                f"SELECT id FROM [{DEFAULT_SCHEMA}].[metadata_archive] "
                "WITH (UPDLOCK, HOLDLOCK) WHERE id = :archive_id"
            ),
            {"archive_id": archive_id},
        ).scalar_one()
        return

    conn.execute(
        select(ArchiveMetadata.id).where(ArchiveMetadata.id == archive_id)
    ).scalar_one()


def calculate_content_hash(file_path: Path, chunk_size: int = 1024 * 1024) -> bytes:
    digest = md5()
    with file_path.open("rb") as file_obj:
        while chunk := file_obj.read(chunk_size):
            digest.update(chunk)
    return digest.digest()


def hash_catalog_file(eng: sa.Engine, file_id: int, file_path: Path) -> bytes:
    content_hash = calculate_content_hash(file_path)
    with eng.begin() as conn:
        conn.execute(
            update(FileMetadata)
            .where(FileMetadata.id == file_id)
            .values(content_hash=content_hash)
        )
    return content_hash


def archive_catalog_signature(members: list[ArchiveMember]) -> str:
    digest = sha1()
    for member in members:
        digest.update(member.relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(member.file_size_bytes).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(member.compressed_size_bytes).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(member.crc32).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(member.zip_depth).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _catalog_signature_from_rows(rows) -> str:
    return archive_catalog_signature(
        [
            ArchiveMember(
                relative_path=row.relative_path,
                file_size_bytes=row.file_size_bytes,
                compressed_size_bytes=row.compressed_size_bytes,
                crc32=row.crc32,
                zip_depth=row.zip_depth,
            )
            for row in rows
        ]
    )


def _existing_member_rows(conn: sa.Connection, archive_id: int):
    return conn.execute(
        select(
            FileMetadata.id,
            FileMetadata.relative_path,
            FileMetadata.file_size_bytes,
            FileMetadata.compressed_size_bytes,
            FileMetadata.crc32,
            FileMetadata.zip_depth,
        ).where(FileMetadata.archive_id == archive_id)
    ).fetchall()


def _insert_member_rows(
    conn: sa.Connection,
    archive_id: int,
    members: list[ArchiveMember],
) -> None:
    if not members:
        return
    conn.execute(
        insert(FileMetadata),
        [
            {
                "archive_id": archive_id,
                "relative_path": member.relative_path,
                "content_hash": None,
                "file_size_bytes": member.file_size_bytes,
                "compressed_size_bytes": member.compressed_size_bytes,
                "crc32": member.crc32,
                "zip_depth": member.zip_depth,
            }
            for member in members
        ],
    )


def catalog_archive(
    eng: sa.Engine,
    archive_id: int,
    zip_path: Path,
    report_progress: Callable[[str], None],
) -> JobRunResult:
    report_progress("Reading archive member catalog...")
    members = iter_archive_members(zip_path)
    if not members:
        logger.warning(
            "Archive %s has no catalogable members: %s", archive_id, zip_path
        )

    seen_paths: set[str] = set()
    duplicate_paths: list[str] = []
    for member in members:
        if member.relative_path in seen_paths:
            duplicate_paths.append(member.relative_path)
        seen_paths.add(member.relative_path)
    if duplicate_paths:
        examples = ", ".join(repr(path) for path in duplicate_paths[:5])
        extra = (
            "" if len(duplicate_paths) <= 5 else f", +{len(duplicate_paths) - 5} more"
        )
        raise ValueError(f"Archive contains duplicate member paths: {examples}{extra}")

    for attempt in range(1, MAX_CATALOG_DEADLOCK_RETRIES + 1):
        try:
            with eng.begin() as conn:
                _lock_archive_row(conn, archive_id)
                catalog_signature = archive_catalog_signature(members)
                existing_rows = _existing_member_rows(conn, archive_id)
                if existing_rows:
                    archive_signature = conn.execute(
                        select(ArchiveMetadata.catalog_signature).where(
                            ArchiveMetadata.id == archive_id
                        )
                    ).scalar_one()
                    existing_signature = (
                        archive_signature or _catalog_signature_from_rows(existing_rows)
                    )
                    if existing_signature != catalog_signature:
                        raise ValueError(
                            "Immutable archive catalog changed for "
                            f"archive {archive_id}."
                        )
                    inserted = 0
                else:
                    _insert_member_rows(conn, archive_id, members)
                    inserted = len(members)
                conn.execute(
                    update(ArchiveMetadata)
                    .where(ArchiveMetadata.id == archive_id)
                    .values(
                        cataloged_at=sa.func.now(),
                        catalog_version=CURRENT_ARCHIVE_CATALOG_VERSION,
                        catalog_signature=catalog_signature,
                    )
                )
            logger.info(
                "Cataloged %s members for archive %s.", len(members), archive_id
            )
            return JobRunResult(
                rows_uploaded=inserted,
                completion_message=f"Cataloged {len(members)} archive members.",
            )
        except Exception as exc:
            if attempt < MAX_CATALOG_DEADLOCK_RETRIES and _is_mssql_deadlock(exc):
                delay = CATALOG_DEADLOCK_RETRY_DELAY_SECONDS * attempt
                logger.warning(
                    "Deadlock cataloging archive %s; retrying in %.1fs (%s/%s).",
                    archive_id,
                    delay,
                    attempt,
                    MAX_CATALOG_DEADLOCK_RETRIES - 1,
                )
                time.sleep(delay)
                continue
            raise

    raise RuntimeError("Archive catalog retry loop exited unexpectedly")
