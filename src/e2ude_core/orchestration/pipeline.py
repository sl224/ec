import logging
import shutil
from concurrent.futures import (
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List, Set

from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ArchiveStateEnum
from e2ude_core.orchestration.state import summarize_archive
from e2ude_core.orchestration.workflow import (
    ArchiveExecutionResult,
    process_staged_archive,
)
from e2ude_core.runtime_files import (
    HANDLED_FILE_SPECS_BY_TYPE,
    FileType,
    build_active_stage_patterns,
)
from e2ude_core.services.zip_io import extract_transport_zip

shutil.COPY_BUFSIZE = 4 * 1024 * 1024

logger = logging.getLogger(__name__)


def _resolve_active_patterns() -> List[str]:
    active_types: Set[FileType] = set(HANDLED_FILE_SPECS_BY_TYPE)
    return build_active_stage_patterns(
        sorted(active_types, key=lambda file_type: file_type.value)
    )


def _worker_process_task(
    archive_id: int, stage_path: Path, db_settings, ctx: EtlContext, db_workers: int
) -> ArchiveExecutionResult:
    local_eng = get_engine(db_settings, default_pool_size=db_workers + 2)

    try:
        return process_staged_archive(
            local_eng,
            archive_id,
            stage_path,
            ctx,
            db_workers=db_workers,
        )
    except Exception as exc:
        logger.error("Worker failed for archive %s: %s", archive_id, exc, exc_info=True)
        return ArchiveExecutionResult(archive_id=archive_id, error=str(exc))
    finally:
        local_eng.dispose()


@dataclass(frozen=True)
class StageOutcome:
    archive_id: int
    skipped: bool = False
    stage_path: Path | None = None
    error: str | None = None


class StagingPipeline:
    """Stage archives in threads and process them in worker processes."""

    def __init__(
        self,
        db_settings,
        archive_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,
        unzip_workers: int = 16,
        process_workers: int = 8,
        db_write_workers: int = 4,
    ):
        self.db_settings = db_settings
        self.archive_id_map = archive_id_map
        self.staging_root = staging_root
        self.unzip_workers = unzip_workers
        self.process_workers = process_workers
        self.db_write_workers = db_write_workers

        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

        self.active_patterns = _resolve_active_patterns()
        self.main_eng = get_engine(db_settings)
        self.skipped_count = 0

    def run(self):
        total = len(self.archive_id_map)
        logger.info("Starting pipeline run. candidates=%s", total)
        logger.info(
            "Workers: io=%s process=%s",
            self.unzip_workers,
            self.process_workers,
        )

        io_pool = ThreadPoolExecutor(
            max_workers=self.unzip_workers, thread_name_prefix="IO_Stage"
        )
        cpu_pool = ProcessPoolExecutor(max_workers=self.process_workers)
        pending_stage = {}
        pending_cpu = {}
        archive_queue = list(self.archive_id_map.keys())
        queue_index = 0

        try:
            with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                pbar.set_postfix(skipped=0, active=0)

                while queue_index < len(archive_queue) or pending_stage or pending_cpu:
                    while (
                        queue_index < len(archive_queue)
                        and not self.stop_event.is_set()
                    ):
                        if not self.buffer_sem.acquire(timeout=0.1):
                            break

                        zip_path = archive_queue[queue_index]
                        queue_index += 1
                        future = io_pool.submit(self._stage_item, zip_path)
                        pending_stage[future] = zip_path

                    active_count = len(pending_stage) + len(pending_cpu)
                    pbar.set_postfix(skipped=self.skipped_count, active=active_count)

                    if not pending_stage and not pending_cpu:
                        continue

                    done, _ = wait(
                        list(pending_stage.keys()) + list(pending_cpu.keys()),
                        timeout=0.5,
                        return_when=FIRST_COMPLETED,
                    )

                    for future in done:
                        if future in pending_stage:
                            pending_stage.pop(future)
                            try:
                                outcome = future.result()
                            except Exception as exc:
                                logger.error("Stage worker exception: %s", exc)
                                self._finalize_item(None, pbar, skipped=False)
                                continue

                            if outcome.skipped:
                                self._finalize_item(None, pbar, skipped=True)
                                continue

                            if outcome.error:
                                logger.error(
                                    "Staging failed for archive %s: %s",
                                    outcome.archive_id,
                                    outcome.error,
                                )
                                self._finalize_item(
                                    outcome.stage_path, pbar, skipped=False
                                )
                                continue

                            cpu_future = cpu_pool.submit(
                                _worker_process_task,
                                outcome.archive_id,
                                outcome.stage_path,
                                self.db_settings,
                                self.ctx,
                                self.db_write_workers,
                            )
                            pending_cpu[cpu_future] = outcome.stage_path
                        else:
                            stage_path = pending_cpu.pop(future)
                            self._handle_cpu_completion(future, stage_path, pbar)
        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted. Force stopping...")
            self.stop_event.set()
            raise
        finally:
            io_pool.shutdown(wait=not self.stop_event.is_set())
            cpu_pool.shutdown(wait=not self.stop_event.is_set())
            self.main_eng.dispose()

    def _stage_item(self, zip_path: Path) -> StageOutcome:
        archive_id = self.archive_id_map.get(zip_path)
        if self.stop_event.is_set():
            return StageOutcome(archive_id=archive_id or -1, error="Pipeline stopped")

        if not archive_id:
            return StageOutcome(
                archive_id=-1, error=f"Archive id missing for {zip_path}"
            )

        try:
            summary = summarize_archive(self.main_eng, archive_id)
            if summary.status == ArchiveStateEnum.UP_TO_DATE:
                return StageOutcome(archive_id=archive_id, skipped=True)
        except Exception as exc:
            logger.warning(
                "State check failed for %s, forcing retry: %s", archive_id, exc
            )

        safe_name = f"{archive_id}_{zip_path.stem}"
        local_dir = self.staging_root / safe_name

        try:
            if local_dir.exists():
                shutil.rmtree(local_dir)
            local_dir.mkdir(parents=True, exist_ok=True)

            temp_zip_local = local_dir / "temp_source.zip"
            shutil.copyfile(zip_path, temp_zip_local)

            extract_transport_zip(
                temp_zip_local,
                local_dir,
                active_patterns=self.active_patterns,
            )
            temp_zip_local.unlink()

            return StageOutcome(archive_id=archive_id, stage_path=local_dir)
        except Exception as exc:
            return StageOutcome(
                archive_id=archive_id,
                stage_path=local_dir,
                error=str(exc),
            )

    def _handle_cpu_completion(self, future, stage_path, pbar):
        try:
            result = future.result()
            if result.error:
                logger.error(
                    "Archive %s failed during processing: %s",
                    result.archive_id,
                    result.error,
                )
        except Exception as exc:
            logger.error("Worker exception: %s", exc)
        finally:
            self._finalize_item(stage_path, pbar, skipped=False)

    def _finalize_item(self, path, pbar, skipped: bool):
        if path:
            try:
                if path.exists():
                    shutil.rmtree(path)
            except OSError:
                pass

        self.buffer_sem.release()

        if skipped:
            self.skipped_count += 1

        pbar.update(1)
