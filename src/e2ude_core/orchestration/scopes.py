import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

from e2ude_core.orchestration.managers import SessionManager, JobControl
from e2ude_core.db.models import StatusEnum
from e2ude_core.context import EtlContext

if TYPE_CHECKING:
    from e2ude_core.orchestration.spec import JobSpec

logger = logging.getLogger(__name__)


@contextmanager
def session_scope(eng, folder_id: int, ctx: EtlContext):
    """
    Context manager that creates and finalizes a `ProcessingSession`.
    Ensures that regardless of success or failure, the session state is finalized.
    """
    session_manager = None
    try:
        session_manager = SessionManager(eng=eng, folder_id=folder_id, ctx=ctx)
        yield session_manager
    except Exception as e:
        logger.error(
            f"Failed to create session manager for FolderID {folder_id}: {e}",
            exc_info=True,
        )
        raise
    finally:
        if session_manager:
            session_manager.finalize_session()


@contextmanager
def job_scope(
    session_manager: SessionManager,
    spec: "JobSpec",
):
    """
    Manages the life-cycle of a specific processing job.

    Handles:
    1. Idempotency Check: Skips if work is already done (checking versions).
    2. Job Creation: Creates a PENDING job record.
    3. Execution Guard: Yields control to the caller to run the actual logic.
    4. Completion: Marks the job as COMPLETED or ERROR.

    Usage:
        with job_scope(manager, spec) as job:
            if job.active:
                # Do work
    """
    job_updater = None

    try:
        # 1. Skip logic: Check global completion (Semantic Invalidation)
        # If we have a file hash, check if this specific target (table) has already been
        # processed for this exact content with a sufficient logic version.
        if spec.hash_id is not None:
            is_completed = session_manager.check_for_completed_job(
                pipeline_id=spec.pipeline_id,
                hash_id=spec.hash_id,
                target_name=spec.target_name,
                required_version=spec.handler_version,
            )
            if is_completed:
                logger.debug(
                    f"Skipping {spec.job_name}: Already Complete (Version {spec.handler_version}+)."
                )
                yield JobControl(manager=None, active=False)
                return

        # 2. Setup: Get or create the job record for this session.
        job_updater = session_manager.get_or_create_job(spec)

        # 3. Check status within *current* session (restart recovery)
        # If the job exists in this session and is already done, we skip it.
        current_status = job_updater.get_status()
        if current_status == StatusEnum.COMPLETED:
            logger.debug(
                f"Skipping {spec.job_name}: already COMPLETED in current session."
            )
            yield JobControl(manager=job_updater, active=False)
            return

        # Mark as Running
        job_updater.mark_running(f"Starting {spec.pipeline_id} processing")

        # 4. Yield Active Control
        # This is where the actual ETL handler runs
        yield JobControl(manager=job_updater, active=True)

        # 5. Success Mark
        # Retrieve rows count if set by the handler during execution
        rows = getattr(job_updater, "_rows_uploaded_in_scope", None)
        job_updater.mark_completed(
            message=f"{spec.pipeline_id} processed successfully",
            rows=rows,
        )

    except Exception as e:
        logger.error(f"Failed to process {spec.job_name}: {e}", exc_info=True)
        if job_updater:
            job_updater.mark_failed(error_message=f"Failed: {e}")
        raise
    finally:
        if job_updater:
            job_updater.close_session()
