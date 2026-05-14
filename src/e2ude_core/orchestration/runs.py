from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
from typing import Callable

import sqlalchemy as sa
from sqlalchemy import func, select

from e2ude_core.context import EtlContext
from e2ude_core.db.models import ProcessingJob, ProcessingSession, StatusEnum
from e2ude_core.runtime_files import FileType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobRunResult:
    rows_uploaded: int = 0
    completion_message: str | None = None
    table_rows: dict[str, int] = field(default_factory=dict)


def cull_stale_runs(eng, max_age: timedelta = timedelta(hours=24)) -> dict[str, int]:
    """Mark abandoned running jobs/sessions as errored."""
    if max_age <= timedelta(0):
        raise ValueError("max_age must be positive")

    cutoff = datetime.utcnow() - max_age
    max_age_hours = int(max_age.total_seconds() // 3600)
    stale_message = f"Culled as stale after exceeding max runtime of {max_age_hours}h"

    try:
        with eng.begin() as conn:
            stale_session_ids = set(
                conn.execute(
                    select(ProcessingSession.id).where(
                        ProcessingSession.status == StatusEnum.RUNNING,
                        ProcessingSession.start_time.is_not(None),
                        ProcessingSession.start_time < cutoff,
                    )
                ).scalars()
            )

            stale_job_ids = set(
                conn.execute(
                    select(ProcessingJob.id).where(
                        ProcessingJob.status == StatusEnum.RUNNING,
                        ProcessingJob.start_time.is_not(None),
                        ProcessingJob.start_time < cutoff,
                    )
                ).scalars()
            )

            if stale_session_ids:
                stale_job_ids.update(
                    conn.execute(
                        select(ProcessingJob.id).where(
                            ProcessingJob.session_id.in_(stale_session_ids),
                            ProcessingJob.status.in_(
                                [StatusEnum.PENDING, StatusEnum.RUNNING]
                            ),
                        )
                    ).scalars()
                )

            if stale_job_ids:
                conn.execute(
                    sa.update(ProcessingJob)
                    .where(ProcessingJob.id.in_(stale_job_ids))
                    .values(
                        status=StatusEnum.ERROR,
                        message=stale_message,
                        end_time=func.now(),
                    )
                )

            if stale_session_ids:
                conn.execute(
                    sa.update(ProcessingSession)
                    .where(ProcessingSession.id.in_(stale_session_ids))
                    .values(status=StatusEnum.ERROR, end_time=func.now())
                )

        return {
            "jobs": len(stale_job_ids),
            "sessions": len(stale_session_ids),
        }
    except Exception:
        logger.error("Failed to cull stale runs.", exc_info=True)
        raise


def create_processing_session(eng, ctx: EtlContext | None = None) -> int:
    values = {
        "git_hash": None if ctx is None else ctx.git_hash,
        "user_name": None if ctx is None else ctx.user_name,
        "host_name": None if ctx is None else ctx.host_name,
        "status": StatusEnum.RUNNING,
    }
    with eng.begin() as conn:
        session_id = conn.execute(
            sa.insert(ProcessingSession)
            .values(**values)
            .returning(ProcessingSession.id)
        ).scalar_one()

    logger.info("Created processing session %s", session_id)
    return session_id


def create_processing_job(
    eng,
    session_id: int,
    *,
    archive_id: int | None = None,
    file_id: int | None = None,
    hash_id: int | None = None,
    file_type: FileType | str | None = None,
    parser_id: str | None = None,
    target_table: str | None = None,
    parser_version: int = 1,
    message: str = "Pending",
) -> int:
    if isinstance(file_type, FileType):
        file_type_value = file_type.value
    else:
        file_type_value = file_type

    with eng.begin() as conn:
        return conn.execute(
            sa.insert(ProcessingJob)
            .values(
                session_id=session_id,
                archive_id=archive_id,
                file_id=file_id,
                hash_id=hash_id,
                file_type=file_type_value,
                parser_id=parser_id,
                target_table=target_table,
                parser_version=parser_version,
                status=StatusEnum.PENDING,
                message=message,
            )
            .returning(ProcessingJob.id)
        ).scalar_one()


def mark_processing_job_running(
    eng,
    job_id: int,
    message: str = "Processing started",
) -> None:
    with eng.begin() as conn:
        conn.execute(
            sa.update(ProcessingJob)
            .where(ProcessingJob.id == job_id)
            .values(
                status=StatusEnum.RUNNING,
                message=message,
                start_time=func.coalesce(ProcessingJob.start_time, func.now()),
            )
        )


def mark_processing_job_completed(
    eng,
    job_id: int,
    message: str = "Completed",
    rows_uploaded: int | None = None,
) -> None:
    with eng.begin() as conn:
        conn.execute(
            sa.update(ProcessingJob)
            .where(ProcessingJob.id == job_id)
            .values(
                status=StatusEnum.COMPLETED,
                message=message,
                end_time=func.now(),
                rows_uploaded=rows_uploaded,
            )
        )


def mark_processing_job_failed(eng, job_id: int, error_message: str) -> None:
    with eng.begin() as conn:
        conn.execute(
            sa.update(ProcessingJob)
            .where(ProcessingJob.id == job_id)
            .values(
                status=StatusEnum.ERROR,
                message=error_message,
                end_time=func.now(),
            )
        )


def run_processing_job(
    eng,
    session_id: int,
    runner: Callable[[Callable[[str], None]], JobRunResult | None],
    *,
    archive_id: int | None = None,
    file_id: int | None = None,
    hash_id: int | None = None,
    file_type: FileType | str | None = None,
    parser_id: str | None = None,
    target_table: str | None = None,
    parser_version: int = 1,
) -> JobRunResult:
    job_id = create_processing_job(
        eng,
        session_id,
        archive_id=archive_id,
        file_id=file_id,
        hash_id=hash_id,
        file_type=file_type,
        parser_id=parser_id,
        target_table=target_table,
        parser_version=parser_version,
    )

    def report_progress(message: str) -> None:
        mark_processing_job_running(eng, job_id, message)

    try:
        report_progress(f"Starting {parser_id or target_table or 'job'}")
        result = runner(report_progress) or JobRunResult()
        mark_processing_job_completed(
            eng,
            job_id,
            message=result.completion_message or "Completed",
            rows_uploaded=result.rows_uploaded,
        )
        return result
    except Exception as exc:
        logger.error("Failed to process job %s: %s", job_id, exc, exc_info=True)
        mark_processing_job_failed(eng, job_id, f"Failed: {exc}")
        raise


def finalize_processing_session(eng, session_id: int, *, failed: bool = False) -> None:
    with eng.begin() as conn:
        failed_jobs = conn.execute(
            select(func.count())
            .select_from(ProcessingJob)
            .where(
                ProcessingJob.session_id == session_id,
                ProcessingJob.status == StatusEnum.ERROR,
            )
        ).scalar_one()
        status = StatusEnum.ERROR if failed or failed_jobs else StatusEnum.COMPLETED
        conn.execute(
            sa.update(ProcessingSession)
            .where(ProcessingSession.id == session_id)
            .values(status=status, end_time=func.now())
        )

    if status == StatusEnum.ERROR:
        logger.warning("Session %s finalized with ERROR.", session_id)
    else:
        logger.info("Session %s finalized COMPLETED.", session_id)
