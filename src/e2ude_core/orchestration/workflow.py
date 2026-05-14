import logging
from dataclasses import dataclass
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.context import EtlContext
from e2ude_core.db.models import FileMetadata
from e2ude_core.orchestration.runs import (
    create_processing_job,
    create_processing_session,
    finalize_processing_session,
    mark_processing_job_completed,
    mark_processing_job_failed,
    mark_processing_job_running,
    run_processing_job,
)
from e2ude_core.orchestration.state import plan_archive_run
from e2ude_core.pipelines.base import process_file
from e2ude_core.pipelines.scanner import (
    SCANNER_PIPELINE_ID,
    SCANNER_VERSION,
    run_metadata_scan,
)

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
) -> ArchiveExecutionResult:
    """Process one staged archive directory end to end."""
    session_id = None
    session_failed = False
    rows_uploaded = 0

    try:
        session_id = create_processing_session(eng, context)
        plan = plan_archive_run(eng, archive_id)

        if plan.needs_scan:
            logger.info("Metadata scan required for archive %s.", archive_id)
            scan_result = run_processing_job(
                eng,
                session_id,
                lambda report_progress: run_metadata_scan(
                    eng, archive_id, staged_path, report_progress
                ),
                archive_id=archive_id,
                parser_id=SCANNER_PIPELINE_ID.value,
                target_table=FileMetadata.__tablename__,
                parser_version=SCANNER_VERSION,
            )
            rows_uploaded += scan_result.rows_uploaded
            plan = plan_archive_run(eng, archive_id)

        if not plan.work_items:
            return ArchiveExecutionResult(
                archive_id=archive_id,
                rows_uploaded=rows_uploaded,
            )

        logger.info("Processing %s pending parser inputs.", len(plan.work_items))
        for work_item in plan.work_items:
            full_path = staged_path / work_item.relative_path
            job_ids = {}
            for model in work_item.target_models:
                job_ids[model.__tablename__] = create_processing_job(
                    eng,
                    session_id,
                    archive_id=archive_id,
                    file_id=work_item.file_id,
                    hash_id=work_item.hash_id,
                    file_type=work_item.file_type,
                    parser_id=work_item.parser_id,
                    target_table=model.__tablename__,
                    parser_version=work_item.parser_version,
                )

            try:
                for table_name, job_id in job_ids.items():
                    mark_processing_job_running(
                        eng,
                        job_id,
                        f"Starting {work_item.parser_id} -> {table_name}",
                    )

                if not full_path.exists():
                    raise FileNotFoundError(f"Staged file missing: {full_path}")

                result = process_file(
                    eng=eng,
                    spec=work_item.spec,
                    hash_id=work_item.hash_id,
                    file_path=full_path,
                    report_progress=lambda _message: None,
                    target_models=work_item.target_models,
                )
                rows_uploaded += result.rows_uploaded
                for table_name, job_id in job_ids.items():
                    mark_processing_job_completed(
                        eng,
                        job_id,
                        message=result.completion_message or "Completed",
                        rows_uploaded=result.table_rows.get(table_name, 0),
                    )
            except Exception as exc:
                session_failed = True
                for job_id in job_ids.values():
                    mark_processing_job_failed(eng, job_id, f"Failed: {exc}")
                raise

        return ArchiveExecutionResult(
            archive_id=archive_id,
            rows_uploaded=rows_uploaded,
        )
    except Exception:
        session_failed = True
        raise
    finally:
        if session_id is not None:
            finalize_processing_session(eng, session_id, failed=session_failed)
