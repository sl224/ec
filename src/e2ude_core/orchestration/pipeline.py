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
from zipfile import ZipFile

from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.orchestration.workflow import (
    FolderExecutionResult,
    process_staged_directory,
)
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.runtime_files import FileType, build_active_stage_patterns

from e2ude_core.orchestration.state import FolderState, summarize_folder
from e2ude_core.pipelines.scanner import SCANNER_VERSION

# Larger copy buffers help SMB reads.
shutil.COPY_BUFSIZE = 4 * 1024 * 1024

logger = logging.getLogger(__name__)


def _resolve_active_patterns() -> List[str]:
    """Build the staged-file pattern set for active handlers."""
    active_types: Set[FileType] = set(HANDLER_REGISTRY.keys())
    return build_active_stage_patterns(
        sorted(active_types, key=lambda file_type: file_type.value)
    )


def _match_any(path_str: str, patterns: List[str]) -> bool:
    p = Path(path_str)
    for pat in patterns:
        if p.match(pat):
            return True
    return False


def _worker_process_task(
    folder_id: int, stage_path: Path, db_settings, ctx: EtlContext, db_workers: int
) -> FolderExecutionResult:
    """Run folder processing in a worker process."""
    local_eng = get_engine(db_settings, default_pool_size=db_workers + 2)

    try:
        return process_staged_directory(
            local_eng,
            folder_id,
            stage_path,
            ctx,
            db_workers=db_workers,
        )
    except Exception as e:
        logger.error("Worker failed for folder %s: %s", folder_id, e, exc_info=True)
        return FolderExecutionResult(folder_id=folder_id, error=str(e))
    finally:
        local_eng.dispose()


@dataclass(frozen=True)
class StageOutcome:
    folder_id: int
    skipped: bool = False
    stage_path: Path | None = None
    error: str | None = None


class StagingPipeline:
    """Stage folders in threads and process them in worker processes."""

    def __init__(
        self,
        db_settings,
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,
        unzip_workers: int = 16,
        process_workers: int = 8,
        db_write_workers: int = 4,
    ):
        self.db_settings = db_settings
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root

        self.unzip_workers = unzip_workers
        self.process_workers = process_workers
        self.db_write_workers = db_write_workers

        # Limit the number of staged folders waiting on local disk.
        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

        self.active_patterns = _resolve_active_patterns()

        self.main_eng = get_engine(db_settings)

        self.skipped_count = 0

    def run(self):
        total = len(self.folder_id_map)
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
        folder_queue = list(self.folder_id_map.keys())
        queue_index = 0

        try:
            with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                pbar.set_postfix(skipped=0, active=0)

                while queue_index < len(folder_queue) or pending_stage or pending_cpu:
                    while (
                        queue_index < len(folder_queue) and not self.stop_event.is_set()
                    ):
                        if not self.buffer_sem.acquire(timeout=0.1):
                            break

                        zip_path = folder_queue[queue_index]
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
                            except Exception as e:
                                logger.error("Stage worker exception: %s", e)
                                self._finalize_item(None, pbar, skipped=False)
                                continue
                            if outcome.skipped:
                                self._finalize_item(None, pbar, skipped=True)
                                continue

                            if outcome.error:
                                logger.error(
                                    "Staging failed for folder %s: %s",
                                    outcome.folder_id,
                                    outcome.error,
                                )
                                self._finalize_item(
                                    outcome.stage_path, pbar, skipped=False
                                )
                                continue

                            cpu_future = cpu_pool.submit(
                                _worker_process_task,
                                outcome.folder_id,
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
        """Stage one archive under the local staging root."""
        folder_id = self.folder_id_map.get(zip_path)
        if self.stop_event.is_set():
            return StageOutcome(folder_id=folder_id or -1, error="Pipeline stopped")

        if not folder_id:
            return StageOutcome(folder_id=-1, error=f"Folder id missing for {zip_path}")

        try:
            summary = summarize_folder(self.main_eng, folder_id, SCANNER_VERSION)
            if summary.status == FolderState.UP_TO_DATE:
                return StageOutcome(folder_id=folder_id, skipped=True)
        except Exception as e:
            logger.warning(f"State check failed for {folder_id}, forcing retry: {e}")

        safe_name = f"{folder_id}_{zip_path.stem}"
        local_dir = self.staging_root / safe_name

        try:
            if local_dir.exists():
                shutil.rmtree(local_dir)
            local_dir.mkdir(parents=True, exist_ok=True)

            temp_zip_local = local_dir / "temp_source.zip"

            shutil.copyfile(zip_path, temp_zip_local)

            with ZipFile(temp_zip_local) as zf:
                members = zf.namelist()
                targets = [
                    m
                    for m in members
                    if _match_any(m, self.active_patterns)
                    or m.endswith("RSM_RawArchive.zip")
                ]
                if targets:
                    zf.extractall(local_dir, members=targets)

            temp_zip_local.unlink()

            for nested_zip in local_dir.rglob("*RSM_RawArchive.zip"):
                try:
                    nested_root = nested_zip.with_suffix("")
                    nested_root.mkdir(exist_ok=True)
                    with ZipFile(nested_zip) as nz:
                        nested_targets = [
                            name
                            for name in nz.namelist()
                            if _match_any(
                                nested_root.name + "/" + name, self.active_patterns
                            )
                        ]
                        if nested_targets:
                            nz.extractall(nested_root, members=nested_targets)
                finally:
                    nested_zip.unlink()

            return StageOutcome(
                folder_id=folder_id,
                stage_path=local_dir,
            )
        except Exception as e:
            return StageOutcome(
                folder_id=folder_id,
                stage_path=local_dir,
                error=str(e),
            )

    def _handle_cpu_completion(self, future, stage_path, pbar):
        """Handle CPU completion in the owning run loop."""
        try:
            result = future.result()
            if result.error:
                logger.error(
                    "Folder %s failed during processing: %s",
                    result.folder_id,
                    result.error,
                )
        except Exception as e:
            logger.error(f"Worker exception: {e}")
        finally:
            self._finalize_item(stage_path, pbar, skipped=False)

    def _finalize_item(self, path, pbar, skipped: bool):
        """Clean up staged files and update progress."""
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
