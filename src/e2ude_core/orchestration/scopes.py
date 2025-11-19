import logging
from contextlib import contextmanager

from e2ude_core.orchestration.managers import SessionManager, JobControl
from e2ude_core.db.models import StatusEnum
from e2ude_core.pipelines.contexts import JobContext
from e2ude_core.context import EtlContext


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


@contextmanager
def session_scope(eng, folder_id: int, ctx: EtlContext):
    """
    Context manager that creates and finalizes a `ProcessingSession`.
    """
    session_manager = None
    try:
        session_manager = SessionManager(eng=eng, folder_id=folder_id, ctx=ctx)
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
    Manages processing life-cycle. Returns a JobControl object.
    Usage:
        with job_scope(mgr, ctx) as job:
            if job.active:
                # do work
    """
    # Unpack parameters from the polymorphic context object.
    pipeline_id = context.handler_instance.PIPELINE_ID
    job_name = context.job_name
    file_type = context.file_type
    file_id = context.file_id
    hash_id = context.hash_id
    dataset_key_str = context.dataset_key

    job_updater = None

    try:
        # 1. Skip logic: Check global completion (idempotency)
        if hash_id is not None:
            is_completed = session_manager.check_for_completed_job(
                pipeline_id=pipeline_id,
                hash_id=hash_id,
                dataset_key=dataset_key_str,
            )
            if is_completed:
                logger.debug(f"Skipping {job_name}: Already Complete.")
                yield JobControl(manager=None, active=False)
                return

        # 2. Setup: Get or create the job record for this session.
        job_updater = session_manager.get_or_create_job(
            job_name=job_name,
            file_type=file_type,
            pipeline_id=pipeline_id,
            file_id=file_id,
            hash_id=hash_id,
            dataset_key=dataset_key_str,
        )

        # 3. Check status within current session (restart recovery)
        current_status = job_updater.get_status()
        if current_status == StatusEnum.COMPLETED:
            logger.debug(f"Skipping {job_name}: already COMPLETED in session.")
            yield JobControl(manager=job_updater, active=False)
            return

        job_updater.mark_running(f"Starting {pipeline_id} processing")

        # 4. Yield Active Control
        yield JobControl(manager=job_updater, active=True)

        # 5. Success Mark
        rows = getattr(job_updater, "_rows_uploaded_in_scope", None)
        job_updater.mark_completed(
            message=f"{pipeline_id} processed successfully",
            rows=rows,
        )

    except Exception as e:
        logger.error(f"Failed to process {job_name}: {e}", exc_info=True)
        if job_updater:
            job_updater.mark_failed(error_message=f"Failed: {e}")
        raise
    finally:
        if job_updater:
            job_updater.close_session()
