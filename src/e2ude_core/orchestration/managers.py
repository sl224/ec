from datetime import datetime, timedelta
import logging
from sqlalchemy import func, select
from sqlalchemy.orm import scoped_session, sessionmaker
from e2ude_core.db.models import (
    StatusEnum,
    ProcessingJob,
    ProcessingSession,
)
from e2ude_core.context import EtlContext
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from e2ude_core.orchestration.spec import JobRunResult, JobSpec

logger = logging.getLogger(__name__)


def cull_stale_runs(eng, max_age: timedelta = timedelta(hours=24)) -> dict[str, int]:
    """
    Marks long-running jobs/sessions as errored so abandoned work does not
    remain RUNNING forever after a process crash or hang.
    """
    if max_age <= timedelta(0):
        raise ValueError("max_age must be positive")

    cutoff = datetime.utcnow() - max_age
    session_factory = scoped_session(sessionmaker(bind=eng))
    session = session_factory()

    try:
        stale_session_ids = set(
            session.execute(
                select(ProcessingSession.id).where(
                    ProcessingSession.status == StatusEnum.RUNNING,
                    ProcessingSession.start_time.is_not(None),
                    ProcessingSession.start_time < cutoff,
                )
            ).scalars()
        )

        stale_job_ids = set(
            session.execute(
                select(ProcessingJob.id).where(
                    ProcessingJob.status == StatusEnum.RUNNING,
                    ProcessingJob.start_time.is_not(None),
                    ProcessingJob.start_time < cutoff,
                )
            ).scalars()
        )

        if stale_session_ids:
            stale_job_ids.update(
                session.execute(
                    select(ProcessingJob.id).where(
                        ProcessingJob.session_id.in_(stale_session_ids),
                        ProcessingJob.status.in_(
                            [StatusEnum.PENDING, StatusEnum.RUNNING]
                        ),
                    )
                ).scalars()
            )

        max_age_hours = int(max_age.total_seconds() // 3600)
        stale_message = (
            f"Culled as stale after exceeding max runtime of {max_age_hours}h"
        )

        if stale_job_ids:
            session.query(ProcessingJob).filter(
                ProcessingJob.id.in_(stale_job_ids)
            ).update(
                {
                    ProcessingJob.status: StatusEnum.ERROR,
                    ProcessingJob.message: stale_message,
                    ProcessingJob.end_time: func.now(),
                },
                synchronize_session=False,
            )

        if stale_session_ids:
            session.query(ProcessingSession).filter(
                ProcessingSession.id.in_(stale_session_ids)
            ).update(
                {
                    ProcessingSession.status: StatusEnum.ERROR,
                    ProcessingSession.end_time: func.now(),
                },
                synchronize_session=False,
            )

        session.commit()

        return {
            "jobs": len(stale_job_ids),
            "sessions": len(stale_session_ids),
        }
    except Exception:
        session.rollback()
        logger.error("Failed to cull stale runs.", exc_info=True)
        raise
    finally:
        session.close()
        session_factory.remove()


class SessionManager:
    """Manages a ProcessingSession and its associated jobs."""

    def __init__(self, eng, folder_id: int, ctx: EtlContext = None):
        if ctx is None:
            user = host = gh = None
        else:
            user = ctx.user_name
            host = ctx.host_name
            gh = ctx.git_hash

        self.eng = eng
        self.Session = scoped_session(sessionmaker(bind=eng))
        self.git_hash = gh
        self.user_name = user
        self.host_name = host
        self.session_id = self._create_session(folder_id)

    def _create_session(self, folder_id: int) -> int:
        session = self.Session()
        try:
            new_session = ProcessingSession(
                git_hash=self.git_hash,
                status=StatusEnum.RUNNING,
                folder_id=folder_id,
                user_name=self.user_name,
                host_name=self.host_name,
            )
            session.add(new_session)
            session.commit()
            logger.info(
                f"Created new session with ID: {new_session.id} for FolderID: {folder_id}"
            )
            return new_session.id
        except Exception as e:
            session.rollback()
            logger.critical(f"Failed to create session: {e}", exc_info=True)
            raise
        finally:
            session.close()

    def _get_or_create_job_id(self, spec: "JobSpec") -> int:
        session = self.Session()
        try:
            existing_job = (
                session.query(ProcessingJob)
                .filter_by(
                    session_id=self.session_id,
                    pipeline_id=spec.pipeline_id.value,
                    file_id=spec.file_id,
                    dataset_key=spec.target_key,
                )
                .first()
            )

            if existing_job:
                return existing_job.id

            new_job = ProcessingJob(
                session_id=self.session_id,
                job_name=spec.job_name,
                pipeline_id=spec.pipeline_id.value,
                status=StatusEnum.PENDING,
                file_id=spec.file_id,
                hash_id=spec.hash_id,
                target_name=spec.target_label,
                dataset_key=spec.target_key,
                handler_version=spec.handler_version,
                file_type=None if spec.file_type is None else spec.file_type.value,
            )
            session.add(new_job)
            session.commit()
            return new_job.id

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to get/create job {spec.job_name}: {e}")
            raise
        finally:
            session.close()

    def _update_job(self, job_id: int, status: StatusEnum, **updates):
        session = self.Session()
        try:
            job = session.get(ProcessingJob, job_id)
            if not job:
                return
            job.status = status
            for key, value in updates.items():
                if key == "start_time" and job.start_time is not None:
                    continue
                setattr(job, key, value)
            session.commit()
            logger.debug("Job %s transitioned to %s", job_id, status)
        except Exception as exc:
            session.rollback()
            logger.error("Failed to transition job %s: %s", job_id, exc)
        finally:
            session.close()

    def _mark_job_running(self, job_id: int, message: str = "Processing started"):
        self._update_job(
            job_id,
            StatusEnum.RUNNING,
            message=message,
            start_time=func.now(),
        )

    def _mark_job_completed(
        self, job_id: int, message: str = "Completed", rows: int | None = None
    ):
        self._update_job(
            job_id,
            StatusEnum.COMPLETED,
            message=message,
            end_time=func.now(),
            rows_uploaded=rows,
        )

    def _mark_job_failed(self, job_id: int, error_message: str):
        self._update_job(
            job_id,
            StatusEnum.ERROR,
            message=error_message,
            end_time=func.now(),
        )

    def run_job(
        self,
        spec: "JobSpec",
        runner: Callable[[Callable[[str], None]], "JobRunResult | None"],
    ) -> "JobRunResult":
        from e2ude_core.orchestration.spec import JobRunResult

        job_id = self._get_or_create_job_id(spec)

        def report_progress(message: str) -> None:
            self._mark_job_running(job_id, message)

        try:
            report_progress(f"Starting {spec.pipeline_id} processing")

            result = runner(report_progress)
            if result is None:
                result = JobRunResult()

            self._mark_job_completed(
                job_id,
                message=result.completion_message
                or f"{spec.pipeline_id} processed successfully",
                rows=result.rows_uploaded,
            )
            return result
        except Exception as e:
            logger.error(f"Failed to process {spec.job_name}: {e}", exc_info=True)
            self._mark_job_failed(job_id, error_message=f"Failed: {e}")
            raise

    def finalize_session(self):
        session = self.Session()
        try:
            session_obj = session.get(ProcessingSession, self.session_id)
            if not session_obj:
                return
            failed_jobs = (
                session.query(ProcessingJob)
                .filter(
                    ProcessingJob.session_id == self.session_id,
                    ProcessingJob.status == StatusEnum.ERROR,
                )
                .count()
            )

            if failed_jobs > 0:
                session_obj.status = StatusEnum.ERROR
                logger.warning(f"Session {self.session_id} finalized with ERROR.")
            else:
                session_obj.status = StatusEnum.COMPLETED
                logger.info(f"Session {self.session_id} finalized COMPLETED.")

            session_obj.end_time = func.now()
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to finalize session {self.session_id}: {e}")
        finally:
            session.close()
            self.Session.remove()
