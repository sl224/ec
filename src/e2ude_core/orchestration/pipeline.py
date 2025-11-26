import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory

logger = logging.getLogger(__name__)


# --- Isolated Worker Function (Must be picklable for Windows/Multiprocessing) ---
def _worker_unzip(zip_path: Path, dest_path: Path) -> bool:
    """
    Pure CPU/IO task run in a separate process to bypass the GIL.
    Returns True on success, raises Exception on failure.
    """
    try:
        # 1. Unpack Outer Archive
        shutil.unpack_archive(str(zip_path), str(dest_path), "zip")

        # 2. Explode Nested Archives (CPU Heavy due to many small headers)
        for nested_zip in dest_path.rglob("*RSM_RawArchive.zip"):
            try:
                shutil.unpack_archive(str(nested_zip), str(nested_zip.parent), "zip")
                nested_zip.unlink()  # Cleanup archive
            except Exception:
                # Log locally or ignore, main process will see files or not
                pass
        return True
    except Exception:
        # Re-raise to be caught by the Future in the parent thread
        raise


class StagingPipeline:
    """
    Continuous Staging Pipeline (Hybrid Thread/Process Model).
    
    - Orchestration: Threads (Shared Memory for Semaphores/DB)
    - Unzipping: Processes (Bypass GIL for high-throughput decompression)
    """

    def __init__(
        self,
        eng: sa.Engine,
        zip_paths: List[Path],
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,      
        network_workers: int = 32,  
        process_workers: int = 8,   
        db_write_workers: int = 4,  
    ):
        self.eng = eng
        self.zip_paths = zip_paths
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root
        
        # Workers Config
        self.network_workers = network_workers
        self.process_workers = process_workers
        self.db_write_workers = db_write_workers
        
        # Flow Control
        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

    def run(self):
        total = len(self.zip_paths)
        logger.info(f"Starting Pipeline. Processing {total} files.")

        # --- Pool Architecture ---
        # 1. Orchestration Pool (Threads): Manages the lifecycle of a staging task.
        #    We use threads here because we need to block on the ProcessPool result
        #    without blocking the main loop.
        orchestrator_pool = ThreadPoolExecutor(
            max_workers=self.network_workers, 
            thread_name_prefix="Orch"
        )
        
        # 2. Heavy Lifting Pool (Processes): Actual decompression.
        #    Bypasses GIL. Matches network_workers count to keep 1:1 mapping.
        unzip_pool = ProcessPoolExecutor(max_workers=self.network_workers)

        # 3. Compute/DB Pool (Threads): Parsing & SQL Uploads.
        #    Must be threads because SQLAlchemy Engines are not process-safe.
        compute_pool = ThreadPoolExecutor(
            max_workers=self.process_workers, 
            thread_name_prefix="DbCpu"
        )

        try:
            with tqdm(total=total, desc="Processing Archives", unit="zip") as pbar:
                for zip_path in self.zip_paths:
                    if self.stop_event.is_set(): break

                    # 1. Acquire Ticket (Backpressure)
                    self.buffer_sem.acquire()

                    # 2. Submit to Orchestrator
                    orchestrator_pool.submit(
                        self._task_orchestrate_file,
                        zip_path,
                        unzip_pool,
                        compute_pool,
                        pbar
                    )

                # Shutdown Sequence
                orchestrator_pool.shutdown(wait=True)
                unzip_pool.shutdown(wait=True)
                compute_pool.shutdown(wait=True)

        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted. Stopping...")
            self.stop_event.set()
            # Force kill pools
            orchestrator_pool.shutdown(wait=False)
            unzip_pool.shutdown(wait=False)
            compute_pool.shutdown(wait=False)
            raise

    def _task_orchestrate_file(
        self, 
        zip_path: Path, 
        unzip_pool: ProcessPoolExecutor,
        compute_pool: ThreadPoolExecutor,
        pbar: tqdm
    ):
        """
        Orchestrates the lifecycle of a single file. 
        Runs in a Thread. Bridges the Process Pool and the Compute Pool.
        """
        if self.stop_event.is_set():
            self._finalize_task(None, pbar)
            return

        folder_id = self.folder_id_map.get(zip_path)
        if not folder_id:
            self._finalize_task(None, pbar)
            return

        local_stage_path = self.staging_root / f"{folder_id}_{zip_path.stem}"

        try:
            # --- Phase 1: Prepare & Unzip (Process Bound) ---
            if local_stage_path.exists():
                shutil.rmtree(local_stage_path)
            local_stage_path.mkdir(parents=True, exist_ok=True)

            # Submit to Process Pool and BLOCK this orchestration thread until done.
            # This is fine because we have `network_workers` amount of threads waiting.
            future = unzip_pool.submit(_worker_unzip, zip_path, local_stage_path)
            future.result() # Raises exception if unzip failed

            # --- Phase 2: Process & Load (Thread Bound) ---
            # We submit to the compute pool. We could wait here, or chain it.
            # Waiting here keeps the logic linear and easier to debug.
            future_proc = compute_pool.submit(
                self._task_process_db,
                folder_id,
                local_stage_path
            )
            future_proc.result()

        except Exception as e:
            logger.error(f"Pipeline failed for {zip_path.name}: {e}")
        finally:
            # --- Phase 3: Cleanup & Release ---
            self._finalize_task(local_stage_path, pbar)

    def _task_process_db(self, folder_id: int, stage_path: Path):
        """
        Actual DB Work. Runs in Compute Pool (Thread).
        """
        if self.stop_event.is_set(): return

        # Pass the DB worker count down to the file processor
        process_staged_directory(
            self.eng, 
            folder_id, 
            stage_path, 
            self.ctx,
            db_workers=self.db_write_workers 
        )

    def _finalize_task(self, path: Path, pbar: tqdm):
        """ Cleanup helper. """
        if path:
            try:
                if path.exists(): shutil.rmtree(path)
            except OSError: pass
        
        self.buffer_sem.release()
        pbar.update(1)