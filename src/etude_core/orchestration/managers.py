import logging

from sqlalchemy import (
    func,
)
from sqlalchemy.orm import scoped_session, sessionmaker
from etude_core.db.models import StatusEnum, ProcessingJob, ProcessingSession
from etude_core.services.zip_io import FileType

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


class SessionManager:
    """Manages a ProcessingSession and its associated jobs."""

    def __init__(self, eng, folder_id: int, git_hash: str = None, user_name=None):
        self.eng = eng
        self.Session = scoped_session(sessionmaker(bind=eng))
        self.git_hash = git_hash
        self.user_name = user_name
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
        job_name: str,
        file_type: FileType,
        pipeline_id: str,
        file_id: int,
        hash_id: int,
        dataset_key: str,  # <-- FIX: Add dataset_key back to signature
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
                    pipeline_id=pipeline_id,
                    file_id=file_id,
                    dataset_key=dataset_key,  # <-- FIX: Filter by key
                )
                .first()
            )

            if existing_job:
                logger.debug(
                    f"Found existing job {existing_job.id} for {pipeline_id} / FileID {file_id} / Key {dataset_key}"
                )
                return JobManager(self.eng, existing_job.id)

            # Create a new job if one doesn't exist for this session.
            logger.info(f"Creating new job for {job_name}")
            new_job = ProcessingJob(
                session_id=self.session_id,
                job_name=job_name,
                file_type=file_type,
                pipeline_id=pipeline_id,
                status=StatusEnum.PENDING,
                file_id=file_id,
                hash_id=hash_id,
                dataset_key=dataset_key,  # <-- FIX: Add key to new job
            )
            self._session.add(new_job)
            self._session.commit()
            logger.info(f"Created new job '{job_name}' with ID: {new_job.id}")

            return JobManager(self.eng, new_job.id)

        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to get or create job {job_name}: {e}", exc_info=True)
            raise

    def check_for_completed_job(
        self,
        pipeline_id: str,
        hash_id: int,
        dataset_key: str,
    ) -> bool:
        """
        Checks if a COMPLETED job exists for a given pipeline, content hash,
        and dataset key across *any* previous session. Used for skipping work.
        """
        session = self.Session()
        try:
            job_exists = (
                session.query(ProcessingJob)
                .filter(
                    ProcessingJob.pipeline_id == pipeline_id,
                    ProcessingJob.hash_id == hash_id,
                    ProcessingJob.dataset_key == dataset_key,
                    ProcessingJob.status == StatusEnum.COMPLETED,
                )
                .first()
            )
            return job_exists is not None
        except Exception as e:
            logger.error(f"Failed skip-job check for hash_id {hash_id}: {e}")
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
