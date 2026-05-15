import logging
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Dict

from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.orchestration.runs import (
    create_processing_session,
    finalize_processing_session,
)
from e2ude_core.orchestration.workflow import ArchiveExecutionResult, process_archive

logger = logging.getLogger(__name__)


def _worker_process_task(
    archive_id: int,
    zip_path: Path,
    staging_root: Path,
    db_settings,
    session_id: int,
) -> ArchiveExecutionResult:
    local_eng = get_engine(db_settings, default_pool_size=4)

    try:
        return process_archive(
            local_eng,
            session_id=session_id,
            archive_id=archive_id,
            zip_path=zip_path,
            staging_root=staging_root,
        )
    except Exception as exc:
        logger.error("Worker failed for archive %s: %s", archive_id, exc, exc_info=True)
        return ArchiveExecutionResult(archive_id=archive_id, error=str(exc))
    finally:
        local_eng.dispose()


class ArchivePipeline:
    """Process archive work in worker processes."""

    def __init__(
        self,
        db_settings,
        archive_id_map: Dict[Path, int],
        staging_root: Path,
        process_workers: int = 8,
    ):
        self.db_settings = db_settings
        self.archive_id_map = archive_id_map
        self.staging_root = staging_root
        self.process_workers = process_workers
        self.ctx = EtlContext.capture()
        self.main_eng = get_engine(db_settings)

    def run(self) -> int:
        total = len(self.archive_id_map)
        session_id = create_processing_session(self.main_eng, self.ctx)
        failed = False
        failed_count = 0
        logger.info("Pipeline starting: archives=%s.", total)
        logger.info("Pipeline workers: processes=%s.", self.process_workers)

        archive_items = list(self.archive_id_map.items())
        next_index = 0
        pending = {}
        max_pending = max(1, self.process_workers * 4)

        try:
            with ProcessPoolExecutor(max_workers=self.process_workers) as pool:
                with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                    pbar.set_postfix(active=0, failed=0)

                    while next_index < total or pending:
                        while next_index < total and len(pending) < max_pending:
                            zip_path, archive_id = archive_items[next_index]
                            next_index += 1
                            future = pool.submit(
                                _worker_process_task,
                                archive_id,
                                zip_path,
                                self.staging_root,
                                self.db_settings,
                                session_id,
                            )
                            pending[future] = archive_id

                        pbar.set_postfix(active=len(pending), failed=failed_count)
                        if not pending:
                            continue

                        done, _ = wait(
                            pending,
                            timeout=0.5,
                            return_when=FIRST_COMPLETED,
                        )

                        for future in done:
                            archive_id = pending.pop(future)
                            try:
                                result = future.result()
                                if result.error:
                                    failed = True
                                    failed_count += 1
                                    logger.error(
                                        "Archive %s failed during processing: %s",
                                        result.archive_id,
                                        result.error,
                                    )
                            except Exception as exc:
                                failed = True
                                failed_count += 1
                                logger.error(
                                    "Worker exception for archive %s: %s",
                                    archive_id,
                                    exc,
                                    exc_info=True,
                                )
                            finally:
                                pbar.update(1)
        except KeyboardInterrupt:
            failed = True
            logger.warning("Pipeline interrupted.")
            raise
        finally:
            finalize_processing_session(self.main_eng, session_id, failed=failed)
            self.main_eng.dispose()

        return failed_count
