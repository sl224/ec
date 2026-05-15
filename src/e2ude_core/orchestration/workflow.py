import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.db.models import FileMetadata
from e2ude_core.orchestration.catalog import (
    CATALOG_PIPELINE_ID,
    HASH_PIPELINE_ID,
    catalog_archive,
    hash_catalog_file,
)
from e2ude_core.orchestration.runs import (
    create_processing_job,
    mark_processing_job_completed,
    mark_processing_job_failed,
    mark_processing_job_running,
    run_processing_job,
    set_processing_job_content_hash,
)
from e2ude_core.orchestration.state import plan_archive_run, target_models_needing_work
from e2ude_core.pipelines.base import process_file
from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION
from e2ude_core.services.zip_io import extract_archive_members

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchiveExecutionResult:
    archive_id: int
    rows_uploaded: int = 0
    error: str | None = None


def _stage_member(zip_path: Path, stage_dir: Path, relative_path: str) -> Path:
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    extracted = extract_archive_members(zip_path, stage_dir, [relative_path])
    if extracted != 1:
        raise FileNotFoundError(
            f"Expected one archive member {relative_path!r}; extracted {extracted}."
        )
    full_path = stage_dir / relative_path
    if not full_path.exists():
        raise FileNotFoundError(f"Staged file missing: {full_path}")
    return full_path


def process_archive(
    eng: sa.Engine,
    *,
    session_id: int,
    archive_id: int,
    zip_path: Path,
    staging_root: Path,
) -> ArchiveExecutionResult:
    """Catalog and process one archive."""
    rows_uploaded = 0

    plan = plan_archive_run(eng, archive_id)
    if plan.needs_catalog:
        logger.info("Archive catalog required for archive %s.", archive_id)
        catalog_result = run_processing_job(
            eng,
            session_id,
            lambda report_progress: catalog_archive(
                eng, archive_id, zip_path, report_progress
            ),
            archive_id=archive_id,
            parser_id=CATALOG_PIPELINE_ID,
            target_table=FileMetadata.__tablename__,
            parser_version=CURRENT_ARCHIVE_CATALOG_VERSION,
        )
        rows_uploaded += catalog_result.rows_uploaded
        plan = plan_archive_run(eng, archive_id)

    if not plan.work_items:
        return ArchiveExecutionResult(
            archive_id=archive_id, rows_uploaded=rows_uploaded
        )

    logger.info("Processing %s pending parser inputs.", len(plan.work_items))
    for item in plan.work_items:
        stage_dir = staging_root / f"archive_{archive_id}_file_{item.file_id}"
        job_id = None
        full_path: Path | None = None
        try:
            content_hash = item.content_hash
            if content_hash is None:
                job_id = create_processing_job(
                    eng,
                    session_id,
                    archive_id=archive_id,
                    file_id=item.file_id,
                    content_hash=None,
                    file_type=item.file_type,
                    parser_id=HASH_PIPELINE_ID,
                    target_table=FileMetadata.__tablename__,
                    parser_version=1,
                )
                mark_processing_job_running(eng, job_id, "Hashing catalog member")
                full_path = _stage_member(zip_path, stage_dir, item.relative_path)
                content_hash = hash_catalog_file(eng, item.file_id, full_path)
                set_processing_job_content_hash(eng, job_id, content_hash)
                mark_processing_job_completed(
                    eng,
                    job_id,
                    message="Hash recorded",
                    rows_uploaded=1,
                )
                job_id = None

            target_models = target_models_needing_work(
                eng,
                content_hash=content_hash,
                spec=item.spec,
                target_models=item.target_models,
            )
            if not target_models:
                continue

            job_id = create_processing_job(
                eng,
                session_id,
                archive_id=archive_id,
                file_id=item.file_id,
                content_hash=content_hash,
                file_type=item.file_type,
                parser_id=item.parser_id,
                target_table=None,
                parser_version=item.parser_version,
            )
            if full_path is None:
                full_path = _stage_member(zip_path, stage_dir, item.relative_path)

            def _progress(message: str) -> None:
                mark_processing_job_running(eng, job_id, message)

            _progress(f"Starting {item.parser_id}")
            result = process_file(
                eng=eng,
                spec=item.spec,
                content_hash=content_hash,
                file_path=full_path,
                report_progress=_progress,
                target_models=target_models,
            )
            rows_uploaded += result.rows_uploaded
            mark_processing_job_completed(
                eng,
                job_id,
                message=result.completion_message or "Completed",
                rows_uploaded=result.rows_uploaded,
            )
        except Exception as exc:
            if job_id is not None:
                mark_processing_job_failed(eng, job_id, f"Failed: {exc}")
            raise
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)

    return ArchiveExecutionResult(archive_id=archive_id, rows_uploaded=rows_uploaded)
