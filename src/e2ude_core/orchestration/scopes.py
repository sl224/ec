import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

from e2ude_core.orchestration.managers import SessionManager, JobControl
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
def job_scope(session_manager: SessionManager, spec: "JobSpec"):
    """
    Manages the life-cycle of a processing job (Logging only).
    Assumption: The orchestrator (workflow.py) has already decided this job NEEDS to run.
    """
    job_updater = None
    try:
        # Setup: Get or create the job record
        job_updater = session_manager.get_or_create_job(spec)

        # Mark Running
        job_updater.mark_running(f"Starting {spec.pipeline_id} processing")

        # Yield Control
        yield JobControl(manager=job_updater, active=True)

        # Mark Success
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
