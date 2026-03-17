import logging
from dataclasses import dataclass
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.orchestration.managers import SessionManager
from e2ude_core.orchestration.spec import JobSpec, build_job_target
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.context import EtlContext

from e2ude_core.pipelines.scanner import (
    run_metadata_scan,
    SCANNER_PIPELINE_ID,
    SCANNER_VERSION,
)
from e2ude_core.pipelines.base import process_file
from e2ude_core.orchestration.state import FolderState, plan_folder_run
from e2ude_core.db.models import FileMetadata

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FolderExecutionResult:
    folder_id: int
    rows_uploaded: int = 0
    error: str | None = None


def process_staged_directory(
    eng: sa.Engine,
    folder_id: int,
    staged_path: Path,
    context: EtlContext,
    db_workers: int = 4,
) -> FolderExecutionResult:
    """Process one staged folder."""
    session_manager = None
    rows_uploaded = 0
    try:
        session_manager = SessionManager(eng=eng, folder_id=folder_id, ctx=context)
        plan = plan_folder_run(eng, folder_id, scan_version=SCANNER_VERSION)

        if plan.summary.status == FolderState.NEEDS_SCAN:
            logger.info(f"Scan Required: {plan.summary.scan_reason}")

            scan_target = build_job_target([FileMetadata])

            scan_spec = JobSpec.for_metadata_scan(
                pipeline_id=SCANNER_PIPELINE_ID,
                job_name=f"MetadataScan: Folder {folder_id}",
                target_label=scan_target.label,
                target_key=scan_target.key,
                handler_version=SCANNER_VERSION,
            )
            scan_result = session_manager.run_job(
                scan_spec,
                lambda report_progress: run_metadata_scan(
                    eng, folder_id, staged_path, report_progress
                ),
            )
            rows_uploaded += scan_result.rows_uploaded

            plan = plan_folder_run(eng, folder_id, scan_version=SCANNER_VERSION)

        if plan.summary.status == FolderState.UP_TO_DATE:
            logger.info("Folder is UP_TO_DATE. No further action required.")
            return FolderExecutionResult(
                folder_id=folder_id, rows_uploaded=rows_uploaded
            )

        logger.info(f"Processing {len(plan.work_items)} files with pending data.")

        for work_item in plan.work_items:
            full_path = staged_path / work_item.relative_path
            if not full_path.exists():
                logger.warning(f"File missing in staging: {full_path}")
                continue

            handler_spec = HANDLER_REGISTRY.get(work_item.file_type)
            if not handler_spec:
                continue

            target = build_job_target(work_item.target_models)

            file_spec = JobSpec.for_file(
                pipeline_id=handler_spec.pipeline_id,
                job_name=f"{handler_spec.pipeline_id}: {work_item.relative_path} [{target.label}]",
                target_label=target.label,
                target_key=target.key,
                handler_version=work_item.handler_version,
                file_type=work_item.file_type,
                file_id=work_item.file_id,
                hash_id=work_item.hash_id,
            )

            file_result = session_manager.run_job(
                file_spec,
                lambda report_progress,
                *,
                current_spec=handler_spec,
                current_hash=work_item.hash_id,
                current_path=full_path,
                current_targets=list(work_item.target_models): process_file(
                    eng=eng,
                    spec=current_spec,
                    hash_id=current_hash,
                    file_path=current_path,
                    report_progress=report_progress,
                    target_models=current_targets,
                    db_workers=db_workers,
                ),
            )
            rows_uploaded += file_result.rows_uploaded
        return FolderExecutionResult(folder_id=folder_id, rows_uploaded=rows_uploaded)
    finally:
        if session_manager is not None:
            session_manager.finalize_session()
