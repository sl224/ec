import logging
import shutil
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory

logger = logging.getLogger(__name__)


# --- Process Worker (Must be module-level for pickling) ---
def _worker_unzip_safe(zip_source: Path, extract_dest: Path) -> bool:
    """
    Pure CPU/Disk task running in a separate process.
    1. Unzips the archive.
    2. Deletes the source archive (freeing space).
    3. Recursively explodes any nested *RSM_RawArchive.zip files.
    """
    try:
        # 1. Unpack Outer
        shutil.unpack_archive(str(zip_source), str(extract_dest), "zip")
        
        # 2. Delete Source Zip (Critical for SSD space management)
        os.remove(zip_source)

        # 3. Explode Nested (CPU Heavy)
        for nested_zip in extract_dest.rglob("*RSM_RawArchive.zip"):
            try:
                # Extract to its parent folder
                shutil.unpack_archive(str(nested_zip), str(nested_zip.parent), "zip")
                nested_zip.unlink()
            except Exception as e:
                # Log but continue (some archives might be corrupt/partial)
                print(f"Warning: Failed to explode nested {nested_zip}: {e}")
        
        return True
    except Exception as e:
        # Clean up mess on failure
        if extract_dest.exists():
            shutil.rmtree(extract_dest, ignore_errors=True)
        raise e


class StagingPipeline:
    """
    High-Throughput 3-Stage Pipeline.
    
    Stages:
    1. Download (Thread): Floods Network. Copy Zip -> SSD.
    2. Unzip (Process): Floods CPU. Extract -> Delete Zip -> Explode Nested.
    3. Ingest (Thread): Floods DB. Parse -> Load -> Delete Files.
    
    Flow Control:
    - Semaphore(N) limits total active items on SSD (Backpressure).
    - Queues between pools absorb rate mismatches (Burst Tolerance).
    """

    def __init__(
        self,
        eng: sa.Engine,
        zip_paths: List[Path],
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,      # Max folders on SSD
        download_workers: int = 32, # Network Flood
        unzip_workers: int = 8,     # CPU Bound (Processes)
        db_workers: int = 8,        # DB Bound (Threads)
        table_write_workers: int = 4 # Parallel tables per file
    ):
        self.eng = eng
        self.zip_paths = zip_paths
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root
        
        # Pools
        self.download_workers = download_workers
        self.unzip_workers = unzip_workers
        self.db_workers = db_workers
        self.table_write_workers = table_write_workers
        
        # Flow Control
        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

    def run(self):
        total = len(self.zip_paths)
        logger.info(f"Starting 3-Stage Pipeline. Processing {total} files.")

        # 1. Network Pool (Threads)
        down_pool = ThreadPoolExecutor(max_workers=self.download_workers, thread_name_prefix="Net")
        
        # 2. Unzip Pool (Processes - Bypasses GIL)
        unzip_pool = ProcessPoolExecutor(max_workers=self.unzip_workers)
        
        # 3. DB Pool (Threads)
        db_pool = ThreadPoolExecutor(max_workers=self.db_workers, thread_name_prefix="DB")

        try:
            with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                
                futures = []
                
                for zip_path in self.zip_paths:
                    if self.stop_event.is_set(): break

                    # 1. Acquire Ticket (Blocks loop if SSD is full)
                    self.buffer_sem.acquire()

                    # 2. Submit to Download Pool
                    f = down_pool.submit(
                        self._task_download,
                        zip_path,
                        unzip_pool,
                        db_pool,
                        pbar
                    )
                    futures.append(f)

                # Wait for entry points to finish
                down_pool.shutdown(wait=True)
                
                # These pools might still have chained work pending
                unzip_pool.shutdown(wait=True)
                db_pool.shutdown(wait=True)

        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted. Force stopping...")
            self.stop_event.set()
            down_pool.shutdown(wait=False)
            unzip_pool.shutdown(wait=False)
            db_pool.shutdown(wait=False)
            raise

    def _task_download(self, zip_path, unzip_pool, db_pool, pbar):
        """ Stage 1: Network Copy (Thread) """
        if self.stop_event.is_set():
            self._finalize_item(None, pbar)
            return

        folder_id = self.folder_id_map.get(zip_path)
        if not folder_id:
            self._finalize_item(None, pbar)
            return

        # Paths
        safe_name = f"{folder_id}_{zip_path.stem}"
        local_dir = self.staging_root / safe_name
        local_zip = self.staging_root / f"{safe_name}.temp_zip"

        try:
            # Clean Prep
            if local_dir.exists(): shutil.rmtree(local_dir)
            if local_zip.exists(): os.remove(local_zip)
            local_dir.mkdir(parents=True, exist_ok=True)

            # A. Flood Network: Copy Raw Bytes
            # shutil.copy2 is optimized for large sequential reads (SMB friendly)
            shutil.copy2(str(zip_path), str(local_zip))

            # B. Submit to Process Pool (Chain)
            # We use add_done_callback logic by proxy: simple chaining in a future
            # Note: ProcessPool futures are thread-safe.
            future = unzip_pool.submit(_worker_unzip_safe, local_zip, local_dir)
            
            # C. Non-blocking Wait? 
            # We want to release THIS download thread immediately.
            # We attach a callback to the Process Future to schedule the DB step.
            future.add_done_callback(
                lambda f: self._on_unzip_complete(f, folder_id, local_dir, db_pool, pbar)
            )

        except Exception as e:
            logger.error(f"Download failed for {zip_path}: {e}")
            if local_zip.exists(): os.remove(local_zip)
            self._finalize_item(local_dir, pbar)

    def _on_unzip_complete(self, future, folder_id, local_dir, db_pool, pbar):
        """ Callback running in a Helper Thread (managed by Future) """
        try:
            # Check for Unzip Exceptions
            future.result() 
            
            # Submit to DB Pool
            db_pool.submit(self._task_ingest, folder_id, local_dir, pbar)
            
        except Exception as e:
            logger.error(f"Unzip failed for ID {folder_id}: {e}")
            self._finalize_item(local_dir, pbar)

    def _task_ingest(self, folder_id, local_dir, pbar):
        """ Stage 3: Database Ingest (Thread) """
        if self.stop_event.is_set():
            self._finalize_item(local_dir, pbar)
            return

        try:
            process_staged_directory(
                self.eng, 
                folder_id, 
                local_dir, 
                self.ctx,
                db_workers=self.table_write_workers
            )
        except Exception as e:
            logger.error(f"Ingest failed for ID {folder_id}: {e}")
        finally:
            self._finalize_item(local_dir, pbar)

    def _finalize_item(self, path, pbar):
        """ Cleanup and Ticket Return """
        if path:
            try:
                if path.exists(): shutil.rmtree(path)
            except OSError: pass
        
        self.buffer_sem.release()
        pbar.update(1)