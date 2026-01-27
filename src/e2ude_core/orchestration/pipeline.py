import logging
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List, Set
from zipfile import ZipFile

from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.orchestration.workflow import process_staged_directory
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.services.zip_io import file_type_patterns

# Import State Logic
from e2ude_core.orchestration.state import get_folder_work_delta, FolderState
from e2ude_core.pipelines.scanner import SCANNER_VERSION

# --- PERFORMANCE TUNING ---
# Increase buffer size to 4MB for faster SMB network copies
shutil.COPY_BUFSIZE = 4 * 1024 * 1024

logger = logging.getLogger(__name__)


def _resolve_active_patterns() -> List[str]:
    """
    Cross-references the Handler Registry with File Patterns.
    """
    active_types: Set[str] = set(HANDLER_REGISTRY.keys())
    patterns = []

    for f_enum, pattern in file_type_patterns.items():
        if f_enum.value in active_types:
            patterns.append(pattern)

    patterns.append("*RSM_RawArchive.zip")
    return patterns


def _match_any(path_str: str, patterns: List[str]) -> bool:
    p = Path(path_str)
    for pat in patterns:
        if p.match(pat):
            return True
    return False


# --- WORKER FUNCTION (Must be at module level for pickling) ---
def _worker_process_task(
    folder_id: int, stage_path: Path, db_settings, ctx: EtlContext, db_workers: int
):
    """
    The CPU-bound task that runs in a separate process.
    It establishes its own Database Engine.
    """
    # 1. Initialize DB Engine for this process
    # We use a smaller pool size per worker since this is a dedicated process
    local_eng = get_engine(db_settings, default_pool_size=db_workers + 2)

    try:
        # 2. Run the Workflow
        process_staged_directory(
            local_eng,
            folder_id,
            stage_path,
            ctx,
            db_workers=db_workers,
        )
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        local_eng.dispose()


class StagingPipeline:
    """
    Hybrid Pipeline: Threaded I/O (Stage 1) + Multiprocess CPU (Stage 2).
    """

    def __init__(
        self,
        db_settings,  # Passed config instead of engine object
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,
        unzip_workers: int = 16,
        process_workers: int = 8,  # Should match CPU core count
        db_write_workers: int = 4,
    ):
        self.db_settings = db_settings
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root

        self.unzip_workers = unzip_workers
        self.process_workers = process_workers
        self.db_write_workers = db_write_workers

        # Semaphore limits the number of folders sitting on the SSD waiting for CPU
        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

        self.active_patterns = _resolve_active_patterns()

        # We need a temporary engine for the Threaded Stage 1 checks
        self.main_eng = get_engine(db_settings)

        self.skipped_count = 0

    def run(self):
        total = len(self.folder_id_map)
        logger.info(f"Starting Hybrid Pipeline. Candidates: {total}")
        logger.info(
            f"Workers: {self.unzip_workers} IO Threads -> {self.process_workers} CPU Processes"
        )

        # Thread Pool for Network I/O
        io_pool = ThreadPoolExecutor(
            max_workers=self.unzip_workers, thread_name_prefix="IO_Stage"
        )

        # Process Pool for CPU-bound Parsing
        cpu_pool = ProcessPoolExecutor(max_workers=self.process_workers)

        try:
            with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                pbar.set_postfix(skipped=0, active=0)

                for i, zip_path in enumerate(self.folder_id_map.keys()):
                    if self.stop_event.is_set():
                        break

                    # Backpressure: Wait if too many folders are staged
                    while not self.buffer_sem.acquire(timeout=0.5):
                        if self.stop_event.is_set():
                            break

                    # Submit Stage 1 (I/O)
                    io_pool.submit(
                        self._task_stage_selective,
                        zip_path,
                        cpu_pool,
                        pbar,
                    )

                # Wait for threads to finish submitting to processes
                io_pool.shutdown(wait=True)
                # Wait for processes to finish parsing
                cpu_pool.shutdown(wait=True)

        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted. Force stopping...")
            self.stop_event.set()
            io_pool.shutdown(wait=False)
            cpu_pool.shutdown(wait=False)
            raise
        finally:
            self.main_eng.dispose()

    def _task_stage_selective(
        self, zip_path: Path, cpu_pool: ProcessPoolExecutor, pbar: tqdm
    ):
        """
        Stage 1: Selective Network Extraction (Running in a Thread).
        """
        if self.stop_event.is_set():
            self._finalize_item(None, pbar, skipped=False)
            return

        folder_id = self.folder_id_map.get(zip_path)
        if not folder_id:
            self._finalize_item(None, pbar, skipped=False)
            return

        # --- JIT STATE CHECK ---
        try:
            delta = get_folder_work_delta(self.main_eng, folder_id, SCANNER_VERSION)
            if delta.status == FolderState.UP_TO_DATE:
                self._finalize_item(None, pbar, skipped=True)
                return
        except Exception as e:
            logger.warning(f"State check failed for {folder_id}, forcing retry: {e}")

        # --- OPTIMIZED EXTRACTION ---
        safe_name = f"{folder_id}_{zip_path.stem}"
        local_dir = self.staging_root / safe_name

        try:
            if local_dir.exists():
                shutil.rmtree(local_dir)
            local_dir.mkdir(parents=True, exist_ok=True)

            # Strategy: Copy compressed file locally first to avoid SMB latency penalty
            temp_zip_local = local_dir / "temp_source.zip"

            # 1. Network Copy (Sequential Read = Fast)
            shutil.copyfile(zip_path, temp_zip_local)

            # 2. Local Extraction (Random Access on SSD = Fast)
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

            # Remove the temp zip immediately to save space
            temp_zip_local.unlink()

            # Handle nested zips locally
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

            # --- HANDOFF TO CPU WORKER ---
            future = cpu_pool.submit(
                _worker_process_task,
                folder_id,
                local_dir,
                self.db_settings,
                self.ctx,
                self.db_write_workers,
            )

            # Callback to handle cleanup and progress bar in the main thread
            future.add_done_callback(
                lambda f: self._on_cpu_task_complete(f, local_dir, pbar)
            )

        except Exception as e:
            logger.error(f"Staging failed for {zip_path}: {e}")
            self._finalize_item(local_dir, pbar, skipped=False)

    def _on_cpu_task_complete(self, future, stage_path, pbar):
        """Callback running in Main Thread/Thread Pool when CPU task finishes"""
        try:
            success, error = future.result()
            if not success:
                logger.error(f"Worker failed: {error}")
        except Exception as e:
            logger.error(f"Worker exception: {e}")
        finally:
            self._finalize_item(stage_path, pbar, skipped=False)

    def _finalize_item(self, path, pbar, skipped: bool):
        """Clean up resources and update UI"""
        if path:
            try:
                if path.exists():
                    shutil.rmtree(path)
            except OSError:
                pass

        self.buffer_sem.release()

        # TQDM is not thread-safe, but usually tolerates simple updates.
        # Ideally use a lock if strictly necessary, but for simple counters it's often fine.
        if skipped:
            self.skipped_count += 1
            if self.skipped_count % 10 == 0:
                pbar.set_postfix(skipped=self.skipped_count)

        pbar.update(1)
