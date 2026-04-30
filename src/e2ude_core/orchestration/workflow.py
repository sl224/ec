import logging
from dataclasses import dataclass
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.context import EtlContext
from e2ude_core.db.models import ArchiveStateEnum, FileMetadata
from e2ude_core.orchestration.managers import SessionManager
from e2ude_core.orchestration.spec import JobSpec, build_job_target
from e2ude_core.orchestration.state import (
    mark_archive_error,
    mark_archive_processing_complete,
    mark_archive_scan_complete,
    plan_archive_run,
)
from e2ude_core.pipelines.base import process_file
from e2ude_core.pipelines.scanner import (
    SCANNER_PIPELINE_ID,
    SCANNER_VERSION,
    run_metadata_scan,
)
from e2ude_core.registry import HANDLER_REGISTRY

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchiveExecutionResult:
    archive_id: int
    rows_uploaded: int = 0
    error: str | None = None


def process_staged_archive(
    eng: sa.Engine,
    archive_id: int,
    staged_path: Path,
    context: EtlContext,
    db_workers: int = 4,
) -> ArchiveExecutionResult:
    """Process one staged archive directory end to end."""
    session_manager = None
    rows_uploaded = 0
    current_state = ArchiveStateEnum.NEEDS_SCAN

    try:
        session_manager = SessionManager(eng=eng, archive_id=archive_id, ctx=context)
        plan = plan_archive_run(eng, archive_id)
        current_state = plan.summary.status

        if plan.summary.status == ArchiveStateEnum.NEEDS_SCAN:
            logger.info("Scan required: %s", plan.summary.work_reason)

            scan_target = build_job_target([FileMetadata])
            scan_spec = JobSpec.for_metadata_scan(
                pipeline_id=SCANNER_PIPELINE_ID,
                job_name=f"MetadataScan: Archive {archive_id}",
                target_label=scan_target.label,
                target_key=scan_target.key,
                handler_version=SCANNER_VERSION,
            )
            scan_result = session_manager.run_job(
                scan_spec,
                lambda report_progress: run_metadata_scan(
                    eng, archive_id, staged_path, report_progress
                ),
            )
            rows_uploaded += scan_result.rows_uploaded
            mark_archive_scan_complete(eng, archive_id)

            plan = plan_archive_run(eng, archive_id)
            current_state = plan.summary.status

        if not plan.work_items:
            mark_archive_processing_complete(eng, archive_id)
            return ArchiveExecutionResult(
                archive_id=archive_id,
                rows_uploaded=rows_uploaded,
            )

        logger.info("Processing %s pending files.", len(plan.work_items))

        for work_item in plan.work_items:
            full_path = staged_path / work_item.relative_path
            if not full_path.exists():
                logger.warning("File missing in staging: %s", full_path)
                continue

            handler_spec = HANDLER_REGISTRY.get(work_item.file_type)
            if not handler_spec:
                continue

            target = build_job_target(work_item.target_models)
            file_spec = JobSpec.for_file(
                pipeline_id=handler_spec.pipeline_id,
                job_name=(
                    f"{handler_spec.pipeline_id}: "
                    f"{work_item.relative_path} [{target.label}]"
                ),
                target_label=target.label,
                target_key=target.key,
                handler_version=work_item.handler_version,
                file_type=work_item.file_type,
                file_id=work_item.file_id,
                hash_id=work_item.hash_id,
            )

            def _run_file_job(
                report_progress,
                *,
                current_spec=handler_spec,
                current_hash=work_item.hash_id,
                current_path=full_path,
                current_targets=list(work_item.target_models),
            ):
                return process_file(
                    eng=eng,
                    spec=current_spec,
                    hash_id=current_hash,
                    file_path=current_path,
                    report_progress=report_progress,
                    target_models=current_targets,
                    db_workers=db_workers,
                )

            file_result = session_manager.run_job(
                file_spec,
                _run_file_job,
            )
            rows_uploaded += file_result.rows_uploaded

        mark_archive_processing_complete(eng, archive_id)
        return ArchiveExecutionResult(
            archive_id=archive_id,
            rows_uploaded=rows_uploaded,
        )
    except Exception as exc:
        mark_archive_error(
            eng,
            archive_id,
            state=current_state,
            error_message=str(exc),
        )
        raise
    finally:
        if session_manager is not None:
            session_manager.finalize_session()
