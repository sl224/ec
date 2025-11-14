import logging
from contextlib import contextmanager

from etude_core.orchestration.managers import SessionManager
from etude_core.db.models import StatusEnum
from etude_core.pipelines.contexts import JobContext


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


@contextmanager
def session_scope(eng, folder_id: int, git_hash: str, user_name: str):
    """
    Context manager that creates and finalizes a `ProcessingSession`.
    """
    session_manager = None
    try:
        session_manager = SessionManager(
            eng=eng, folder_id=folder_id, git_hash=git_hash, user_name=user_name
        )
        yield session_manager
    except Exception as e:
        logger.error(
            f"Failed to even create session manager for FolderID {folder_id}: {e}",
            exc_info=True,
        )
        raise
    finally:
        if session_manager:
            session_manager.finalize_session()


@contextmanager
def job_scope(
    session_manager: SessionManager,
    context: JobContext,
):
    """
    Manages the lifecycle of a single `ProcessingJob`. It handles idempotency
    by checking for previously completed jobs, creates a job record, yields a
    `JobManager`, and guarantees status updates (COMPLETED or ERROR) on exit.
    """
    # Unpack parameters from the polymorphic context object.
    pipeline_id = context.handler_instance.PIPELINE_ID
    job_name = context.job_name
    file_type = context.file_type
    file_id = context.file_id
    hash_id = context.hash_id
    dataset_key_str = context.dataset_key  # Already a string

    job_updater = None  # Ensure it's defined in all paths
    should_skip = False  # Ensure it's defined

    try:
        # Skip logic: check if a completed job for this content hash already exists.
        if hash_id is None:
            logger.debug(f"Job {job_name} has no hash_id, will run.")
            is_already_completed = False
        else:
            is_already_completed = session_manager.check_for_completed_job(
                pipeline_id=pipeline_id,
                hash_id=hash_id,
                dataset_key=dataset_key_str,
            )

        if is_already_completed:
            logger.debug(
                f"Skipping {job_name}: Found COMPLETED job in a previous session for hash_id {hash_id}."
            )
            should_skip = True
            yield None, True  # Yield (updater=None, should_skip=True)
            return  # Exit context manager early

        # Setup: Get or create the job record for this session.
        job_updater = session_manager.get_or_create_job(
            job_name=job_name,
            file_type=file_type,
            pipeline_id=pipeline_id,
            file_id=file_id,
            hash_id=hash_id,
            dataset_key=dataset_key_str,
        )

        current_status = job_updater.get_status()
        if current_status == StatusEnum.COMPLETED:
            logger.debug(
                f"Skipping {job_name}: already COMPLETED (within this session)."
            )
            should_skip = True
            yield job_updater, True
            return

        job_updater.mark_running(f"Starting {pipeline_id} processing")

        # Yield (updater, should_skip=False)
        yield job_updater, False

    except Exception as e:
        # Error handling: mark the job as failed.
        logger.error(f"Failed to process {job_name}: {e}", exc_info=True)
        if job_updater:
            job_updater.mark_failed(error_message=f"Failed: {e}")
        # Re-raise exception to be handled by the outer session scope
        raise
    else:
        # Success: mark the job as completed if it wasn't skipped.
        if job_updater and not should_skip:
            rows = getattr(job_updater, "_rows_uploaded_in_scope", None)

            job_updater.mark_completed(
                message=f"{pipeline_id} processed successfully",
                rows=rows,
            )
    finally:
        # Teardown: close the job's database session.
        if job_updater:
            job_updater.close_session()
