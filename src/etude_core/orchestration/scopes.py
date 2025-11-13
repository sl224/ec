import logging
from contextlib import contextmanager

# Core orchestration components for job status tracking
from etude_core.orchestration.managers import SessionManager

# Import type hints
from etude_core.pipelines.protocols import PipelineJob
from etude_core.pipelines.scanner import FileToProcess
from etude_core.db.models import StatusEnum


logger = logging.getLogger(__name__)


@contextmanager
def session_scope(eng, folder_id: int, git_hash: str, user_name: str):
    """
    Manages the creation and finalization of a ProcessingSession.

    Creates a SessionManager on entry and guarantees .finalize_session() is
    called on exit to correctly set the session status (COMPLETED or ERROR).
    """
    logger.info(f"--- Opening Session for FolderID {folder_id} ---")
    session_manager = None
    try:
        session_manager = SessionManager(
            eng=eng, folder_id=folder_id, git_hash=git_hash, user_name=user_name
        )
        # Yield the manager to be used inside the 'with' block
        yield session_manager
    except Exception as e:
        logger.error(
            f"Failed to even create session manager for FolderID {folder_id}: {e}",
            exc_info=True,
        )
        # Re-raise, as nothing else can be done
        raise
    finally:
        if session_manager:
            # Teardown logic: query all child jobs and set the session
            # status to COMPLETED or ERROR accordingly.
            session_manager.finalize_session()
            logger.info(
                f"--- Session {session_manager.session_id} finalized for FolderID {folder_id} ---"
            )


@contextmanager
def job_scope(
    session_manager: SessionManager,
    handler_instance: PipelineJob,  # <--- Use Protocol (Supports Scanner & FileHandler)
    file_to_process: FileToProcess = None,  # The file object (optional)
):
    """
    Manages the lifecycle of a single ProcessingJob (per file).

    Handles idempotency by checking for previously completed jobs based on hash_id.
    It creates a job record, yields a JobUpdater, and guarantees that the
    job status is set to COMPLETED or ERROR upon exit.
    """

    # --- Unpack parameters ---
    # Safe because PipelineJob protocol guarantees this attribute exists
    pipeline_id = handler_instance.PIPELINE_ID

    if file_to_process:
        # This is a file-level job (e.g., TmptrLogHandler)
        job_name = f"{pipeline_id}: {file_to_process.relative_path}"
        file_type = file_to_process.file_type
        file_id = file_to_process.file_id
        hash_id = file_to_process.hash_id
    else:
        # This is a folder-level job (e.g., MetadataScanHandler)
        # We rely on the handler instance having 'folder_id'
        folder_id = getattr(handler_instance, "folder_id", "UnknownFolder")
        job_name = f"{pipeline_id}: FolderID {folder_id}"
        file_type = "N/A"
        file_id = None
        hash_id = None

    try:
        # --- SKIP LOGIC ---
        # Check if this pipeline/hash combo has ever been completed successfully.
        if hash_id is None:  # Catches folder-level jobs (like MetadataScan)
            logger.debug(f"Job {job_name} has no hash_id, will run.")
            is_already_completed = False
        else:
            is_already_completed = session_manager.check_for_completed_job(
                pipeline_id=pipeline_id, hash_id=hash_id
            )

        if is_already_completed:
            logger.debug(
                f"Skipping {job_name}: Found COMPLETED job in a previous session for hash_id {hash_id}."
            )
            yield None, True  # Yield (updater=None, should_skip=True)
            return  # Exit context manager early

        # 'Setup' logic
        # Not completed, so create a job for *this* session
        job_updater = session_manager.get_or_create_job(
            job_name=job_name,
            file_type=file_type,
            pipeline_id=pipeline_id,
            file_id=file_id,
            hash_id=hash_id,
        )

        # This check is still valid for re-runs *within the same session*
        current_status = job_updater.get_status()
        if current_status == StatusEnum.COMPLETED:
            logger.debug(
                f"Skipping {job_name}: already COMPLETED (within this session)."
            )
            yield job_updater, True
            return

        # --- REFACTOR 1: Use explicit method ---
        job_updater.mark_running(f"Starting {pipeline_id} processing")

        # Yield (updater, should_skip=False)
        yield job_updater, False

    except Exception as e:
        # 'Error' logic
        logger.error(f"Failed to process {job_name}: {e}", exc_info=True)
        if job_updater:
            # --- REFACTOR 2: Use explicit method ---
            job_updater.mark_failed(error_message=f"Failed: {e}")
        # Re-raise exception to be handled by the outer session scope
        raise
    else:
        # 'Success' logic (no exception occurred)
        if job_updater:  # Only update if we actually ran the job
            # Retrieve the row count, if the handler set it
            rows = getattr(job_updater, "_rows_uploaded_in_scope", None)

            # --- REFACTOR 3: Use explicit method ---
            job_updater.mark_completed(
                message=f"{pipeline_id} processed successfully",
                rows=rows,
            )
    finally:
        # 'Teardown' logic
        if job_updater:
            job_updater.close_session()
