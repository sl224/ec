import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory

logger = logging.getLogger(__name__)


class StagingPipeline:
    """
    Continuous Staging Pipeline.
    Uses a 'Token Bucket' (Semaphore) to maintain a constant buffer of work 
    on the SSD, maximizing throughput without barriers.
    """

    def __init__(
        self,
        eng: sa.Engine,
        zip_paths: List[Path],
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,      # Max folders on SSD at once
        network_workers: int = 32,  # High I/O concurrency
        process_workers: int = 8,   # High CPU/DB concurrency
        db_write_workers: int = 4,  # Parallel Tables per File
    ):
        self.eng = eng
        self.zip_paths = zip_paths
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root
        self.network_workers = network_workers
        self.process_workers = process_workers
        self.db_write_workers = db_write_workers
        
        # The "Ticket System". You need a ticket to use SSD space.
        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

    def run(self):
        total = len(self.zip_paths)
        logger.info(f"Starting Continuous Pipeline. Processing {total} files.")

        # Separate pools prevents heavy CPU tasks from blocking network ACKs
        network_pool = ThreadPoolExecutor(max_workers=self.network_workers, thread_name_prefix="Net")
        compute_pool = ThreadPoolExecutor(max_workers=self.process_workers, thread_name_prefix="Cpu")

        try:
            with tqdm(total=total, desc="Processing Archives", unit="zip") as pbar:
                for zip_path in self.zip_paths:
                    if self.stop_event.is_set(): break

                    # 1. Acquire Ticket (Blocks if SSD is full)
                    self.buffer_sem.acquire()

                    # 2. Submit Chain (Non-blocking)
                    network_pool.submit(
                        self._task_stage,
                        zip_path,
                        compute_pool,
                        pbar
                    )

                # Wait for the pools to drain naturally
                network_pool.shutdown(wait=True)
                compute_pool.shutdown(wait=True)

        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted. Stopping...")
            self.stop_event.set()
            network_pool.shutdown(wait=False)
            compute_pool.shutdown(wait=False)
            raise

    def _task_stage(self, zip_path: Path, compute_pool: ThreadPoolExecutor, pbar: tqdm):
        """ Phase 1: Network Unzip (I/O Bound) """
        if self.stop_event.is_set():
            self._finalize_task(None, pbar)
            return

        folder_id = self.folder_id_map.get(zip_path)
        if not folder_id:
            self._finalize_task(None, pbar)
            return

        local_stage_path = self.staging_root / f"{folder_id}_{zip_path.stem}"

        try:
            # Prepare Staging
            if local_stage_path.exists(): shutil.rmtree(local_stage_path)
            local_stage_path.mkdir(parents=True, exist_ok=True)

            # Heavy I/O: Pull from Network
            shutil.unpack_archive(str(zip_path), str(local_stage_path), "zip")

            # Explode Nested Zips (Flattening)
            for nested_zip in local_stage_path.rglob("*RSM_RawArchive.zip"):
                try:
                    shutil.unpack_archive(str(nested_zip), str(nested_zip.parent), "zip")
                    nested_zip.unlink()
                except Exception: pass

            # Hand off to Compute Pool
            compute_pool.submit(self._task_process, folder_id, local_stage_path, pbar)

        except Exception as e:
            logger.error(f"Failed to stage {zip_path}: {e}")
            self._finalize_task(local_stage_path, pbar)

    def _task_process(self, folder_id: int, stage_path: Path, pbar: tqdm):
        """ Phase 2: Processing (CPU/DB Bound) """
        if self.stop_event.is_set():
            self._finalize_task(stage_path, pbar)
            return

        try:
            # The db_write_workers arg triggers parallel table uploads
            process_staged_directory(
                self.eng, 
                folder_id, 
                stage_path, 
                self.ctx,
                db_workers=self.db_write_workers 
            )
        except Exception as e:
            logger.error(f"Failed processing {folder_id}: {e}")
        finally:
            self._finalize_task(stage_path, pbar)

    def _finalize_task(self, path: Path, pbar: tqdm):
        """ Phase 3: Cleanup & Ticket Release """
        if path:
            try:
                if path.exists(): shutil.rmtree(path)
            except OSError: pass
        
        # Critical: Release ticket to let the Main Thread pull the next zip
        self.buffer_sem.release()
        pbar.update(1)