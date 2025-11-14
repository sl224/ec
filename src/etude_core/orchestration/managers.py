import logging

from sqlalchemy import (
    func,
)
from sqlalchemy.orm import scoped_session, sessionmaker
from etude_core.db.models import StatusEnum, ProcessingJob, ProcessingSession
from etude_core.services.zip_io import FileType

logger = logging.getLogger(__name__)


# --- Manager Classes ---


class JobManager:
    # ... (NO CHANGES TO JobManager) ...
    def __init__(self, eng, job_id: int):
        self.eng = eng
        self.job_id = job_id
        self.Session = scoped_session(sessionmaker(bind=eng))
        self._session = self.Session()  # Keep a persistent session

        # Handlers will set this attribute directly.
        self._rows_uploaded_in_scope = None

    def get_status(self) -> StatusEnum | None:
        """Fetches the current status of the job from the DB."""
        try:
            # Refresh from DB to get the most current state
            self._session.expire_all()  # Invalidate cache
            job = self._session.query(ProcessingJob).get(self.job_id)
            return job.status if job else None
        except Exception as e:
            logger.error(f"Failed to get status for job {self.job_id}: {e}")
            self._session.rollback()
            return None

    def mark_running(self, message: str = "Processing started"):
        """Transition job to RUNNING state and set start timer."""
        self._transition(
            status=StatusEnum.RUNNING,
            message=message,
            start_time=func.now(),  # Only set start_time here
        )

    def mark_completed(self, message: str = "Completed", rows: int = None):
        """Transition job to COMPLETED state, set end timer, and record metrics."""
        # Encapsulate the fallback logic for rows inside the 'Success' event only
        final_rows = rows if rows is not None else self._rows_uploaded_in_scope

        self._transition(
            status=StatusEnum.COMPLETED,
            message=message,
            end_time=func.now(),
            rows_uploaded=final_rows,
        )

    def mark_failed(self, error_message: str):
        """Transition job to ERROR state and set end timer."""
        self._transition(
            status=StatusEnum.ERROR, message=error_message, end_time=func.now()
        )

    def _transition(self, status: StatusEnum, **updates):
        """
        Internal helper to execute the DB write.
        This is private (_) because external callers shouldn't use generic updates.
        """
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
        """Closes the persistent session."""
        if self._session:
            self._session.close()
        self.Session.remove()


class SessionManager:
    # ... (NO CHANGES TO __init__ or _create_session) ...
    def __init__(self, eng, folder_id: int, git_hash: str = None, user_name=None):
        self.eng = eng
        # Use a scoped_session for thread-safety and consistency
        self.Session = scoped_session(sessionmaker(bind=eng))
        self.git_hash = git_hash
        self.user_name = user_name
        self.session_id = self._create_session(folder_id)
        self._session = self.Session()  # Keep a persistent session

    def _create_session(self, folder_id: int) -> int:
        """Creates a new session row in the DB and returns its ID."""
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
        dataset_key: str,  # <-- This is already correct (accepts string)
    ) -> JobManager:
        """
        Idempotently gets or creates a job for a specific pipeline and file.
        Returns a JobManager for it.
        """
        try:
            # 1. Try to find an existing job *in this session*
            existing_job = (
                self._session.query(ProcessingJob)
                .filter_by(
                    session_id=self.session_id,
                    pipeline_id=pipeline_id,
                    file_id=file_id,
                    dataset_key=dataset_key,  # <-- FIX: Find by key too
                )
                .first()
            )

            if existing_job:
                logger.debug(
                    f"Found existing job {existing_job.id} for {pipeline_id} / FileID {file_id} / Key {dataset_key}"
                )
                # Return a JobManager for the *existing* job
                return JobManager(self.eng, existing_job.id)

            # 2. Create a new job if not found
            logger.info(f"Creating new job for {job_name}")
            new_job = ProcessingJob(
                session_id=self.session_id,
                job_name=job_name,
                file_type=file_type,
                pipeline_id=pipeline_id,
                status=StatusEnum.PENDING,
                file_id=file_id,
                hash_id=hash_id,
                dataset_key=dataset_key,  # <-- This is already correct (stores string)
            )
            self._session.add(new_job)
            self._session.commit()
            logger.info(f"Created new job '{job_name}' with ID: {new_job.id}")

            # Return a JobManager for the *new* job
            return JobManager(self.eng, new_job.id)

        except Exception as e:
            self._session.rollback()
            logger.error(f"Failed to get or create job {job_name}: {e}", exc_info=True)
            raise

    def check_for_completed_job(
        self,
        pipeline_id: str,
        hash_id: int,
        dataset_key: str,  # <-- FIX: Add
    ) -> bool:
        """
        Checks if a COMPLETED job exists for this pipeline/hash/key combo
        in *any* session.
        """
        # Use a fresh session for this check
        session = self.Session()
        try:
            # We look for a hash_id in *any* session that has
            # been completed for this specific pipeline.
            job_exists = (
                session.query(ProcessingJob)
                .filter(
                    ProcessingJob.pipeline_id == pipeline_id,
                    ProcessingJob.hash_id == hash_id,
                    # --- FIX: Add dataset_key to the check ---
                    ProcessingJob.dataset_key == dataset_key,
                    ProcessingJob.status == StatusEnum.COMPLETED,
                )
                .first()
            )

            return job_exists is not None
        except Exception as e:
            logger.error(f"Failed skip-job check for hash_id {hash_id}: {e}")
            session.rollback()
            return False  # Failsafe: run the job
        finally:
            session.close()

    def finalize_session(self):
        # ... (NO CHANGES to finalize_session) ...
        """Updates the parent session status based on all child jobs."""
        session = self.Session()  # Use a fresh session for finalization
        try:
            session_obj = session.query(ProcessingSession).get(self.session_id)
            if not session_obj:
                logger.error(f"Session {self.session_id} not found during finalize.")
                return

            # Check for any failed jobs in this session
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
            # Clean up the scoped sessions
            if self._session:
                self._session.close()
            self.Session.remove()
