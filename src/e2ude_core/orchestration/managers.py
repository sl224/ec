import logging
from dataclasses import dataclass
from sqlalchemy import func
from sqlalchemy.orm import scoped_session, sessionmaker
from e2ude_core.db.models import (
    StatusEnum,
    ProcessingJob,
    ProcessingSession,
    FileMetadata,
)
from e2ude_core.context import EtlContext
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from e2ude_core.orchestration.spec import JobSpec

logger = logging.getLogger(__name__)


class JobManager:
    """Manages the state of a single ProcessingJob in the database."""

    def __init__(self, eng, job_id: int):
        self.eng = eng
        self.job_id = job_id
        self.Session = scoped_session(sessionmaker(bind=eng))
        self._session = self.Session()
        self._rows_uploaded_in_scope = None

    def get_status(self) -> StatusEnum | None:
        try:
            self._session.expire_all()
            job = self._session.query(ProcessingJob).get(self.job_id)
            return job.status if job else None
        except Exception as e:
            logger.error(f"Failed to get status for job {self.job_id}: {e}")
            self._session.rollback()
            return None

    def mark_running(self, message: str = "Processing started"):
        self._transition(
            status=StatusEnum.RUNNING,
            message=message,
            start_time=func.now(),
        )

    def mark_completed(self, message: str = "Completed", rows: int = None):
        final_rows = rows if rows is not None else self._rows_uploaded_in_scope
        self._transition(
            status=StatusEnum.COMPLETED,
            message=message,
            end_time=func.now(),
            rows_uploaded=final_rows,
        )

    def mark_failed(self, error_message: str):
        self._transition(
            status=StatusEnum.ERROR, message=error_message, end_time=func.now()
        )

    def _transition(self, status: StatusEnum, **updates):
        try:
            job = self._session.get(ProcessingJob, self.job_id)
            if not job:
                return
            job.status = status
            for key, value in updates.items():
                setattr(job, key, value)
            self._session.commit()
            logger.debug(f"Job {self.job_id} transitioned to {status}")
        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to transition job {self.job_id}: {e}")

    def close_session(self):
        if self._session:
            self._session.close()
        self.Session.remove()


@dataclass
class JobControl:
    """Yielded by job_scope to allow clean conditional execution."""

    manager: Optional["JobManager"]
    active: bool


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
        self._session = self.Session()

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

    def get_or_create_job(
        self,
        spec: "JobSpec",
    ) -> JobManager:
        """
        Idempotently gets an existing job or creates a new one for a specific
        pipeline, file, and dataset. Returns a JobManager for it.
        """
        try:
            # Try to find an existing job within the current session.
            existing_job = (
                self._session.query(ProcessingJob)
                .filter_by(
                    session_id=self.session_id,
                    pipeline_id=spec.pipeline_id,
                    file_id=spec.file_id,
                    target_name=spec.target_name,
                )
                .first()
            )

            if existing_job:
                logger.debug(
                    f"Found existing job {existing_job.id} for {spec.pipeline_id} / FileID {spec.file_id} / Target {spec.target_name}"
                )
                return JobManager(self.eng, existing_job.id)

            # Create a new job if one doesn't exist for this session.
            logger.info(f"Creating new job for {spec.job_name}")
            new_job = ProcessingJob(
                session_id=self.session_id,
                job_name=spec.job_name,
                pipeline_id=spec.pipeline_id,
                status=StatusEnum.PENDING,
                file_id=spec.file_id,
                target_name=spec.target_name,
                handler_version=spec.handler_version,
            )
            self._session.add(new_job)
            self._session.commit()
            logger.info(f"Created new job '{spec.job_name}' with ID: {new_job.id}")

            return JobManager(self.eng, new_job.id)

        except Exception as e:
            self._session.rollback()
            logger.error(
                f"Failed to get or create job {spec.job_name}: {e}", exc_info=True
            )
            raise

    def check_for_completed_job(
        self,
        pipeline_id: str,
        hash_id: int,
        target_name: str,
        required_version: int,
    ) -> bool:
        """
        Checks if a COMPLETED job exists for a given pipeline, content hash,
        and target name across *any* previous session.

        Used for skipping work (Semantic Invalidation).
        Returns True ONLY if work exists for this hash/target WITH a version >= required_version.
        """
        session = self.Session()
        try:
            # Find the MAX version successfully processed for this content
            best_existing_version = (
                session.query(func.max(ProcessingJob.handler_version))
                .join(FileMetadata, ProcessingJob.file_id == FileMetadata.id)
                .filter(
                    ProcessingJob.pipeline_id == pipeline_id,
                    ProcessingJob.target_name == target_name,
                    ProcessingJob.status == StatusEnum.COMPLETED,
                    FileMetadata.hash_id == hash_id,  # Check hash via join
                )
                .scalar()
            )

            if best_existing_version is None:
                return False  # Never processed

            # Professional Semantic Invalidation:
            # If we have processed v2, and we are asking for v2, SKIP (True).
            # If we have processed v1, and we are asking for v2, RUN (False).
            return best_existing_version >= required_version

        except Exception as e:
            logger.error(f"Skip check failed for hash_id {hash_id}: {e}")
            session.rollback()
            return False
        finally:
            session.close()

    def finalize_session(self):
        """
        Finalizes the session, setting its status to COMPLETED or ERROR.
        """
        session = self.Session()
        try:
            session_obj = session.query(ProcessingSession).get(self.session_id)
            if not session_obj:
                logger.error(f"Session {self.session_id} not found during finalize.")
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
            if self._session:
                self._session.close()
            self.Session.remove()
