import logging
import shutil
import concurrent.futures
from pathlib import Path
from typing import Dict, List
import time

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory

logger = logging.getLogger(__name__)

class StagingPipeline:
    """
    Batch-Oriented Pipeline.
    Processes data in discrete chunks to maximize network throughput 
    while strictly controlling disk usage.
    """
    def __init__(
        self,
        eng: sa.Engine,
        zip_paths: List[Path],
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        batch_size: int = 30,       # How many zips to bring to SSD at once
        unzip_workers: int = 32,    # High concurrency for Network I/O
        process_workers: int = 8,   # Lower concurrency for DB/CPU
    ):
        self.eng = eng
        self.zip_paths = zip_paths
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root
        self.batch_size = batch_size
        self.unzip_workers = unzip_workers
        self.process_workers = process_workers
        self.ctx = EtlContext.capture()

    def run(self):
        total_zips = len(self.zip_paths)
        logger.info(f"Starting Batch Pipeline. Processing {total_zips} files in batches of {self.batch_size}.")

        # Chunk the work
        batches = [
            self.zip_paths[i : i + self.batch_size]
            for i in range(0, len(self.zip_paths), self.batch_size)
        ]

        with tqdm(total=total_zips, desc="Processing Archives", unit="zip") as pbar:
            for batch_idx, batch in enumerate(batches):
                logger.info(f"--- Starting Batch {batch_idx + 1}/{len(batches)} ({len(batch)} files) ---")
                
                # 1. Parallel Stage (Unzip)
                # We map source zips to their staged directory paths
                # Return: List of (folder_id, stage_path) tuples for successful unzips
                staged_items = self._phase_stage_batch(batch)
                
                if not staged_items:
                    pbar.update(len(batch)) # Mark all as failed/skipped
                    continue

                # 2. Parallel Process (DB Load)
                self._phase_process_batch(staged_items, pbar)

                # 3. Cleanup (Bulk Delete)
                # We clean up immediately to free space for the next batch
                self._phase_cleanup_batch(staged_items)

    def _phase_stage_batch(self, batch: List[Path]) -> List[tuple]:
        """
        Phase 1: Flood the network with read requests to bring files to SSD.
        """
        staged_results = []
        
        # We use a high thread count here because we are waiting on Network Latency
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.unzip_workers) as executor:
            future_to_zip = {
                executor.submit(self._task_unzip_one, zp): zp 
                for zp in batch
            }

            for future in concurrent.futures.as_completed(future_to_zip):
                zip_path = future_to_zip[future]
                try:
                    result = future.result() # returns (folder_id, stage_path) or None
                    if result:
                        staged_results.append(result)
                except Exception as e:
                    logger.error(f"Failed to stage {zip_path}: {e}")
        
        return staged_results

    def _task_unzip_one(self, zip_path: Path):
        """Worker function for unzipping a single file."""
        folder_id = self.folder_id_map.get(zip_path)
        if not folder_id:
            return None

        safe_name = f"{folder_id}_{zip_path.stem}"
        local_stage_path = self.staging_root / safe_name

        # Clean slate
        if local_stage_path.exists():
            shutil.rmtree(local_stage_path)
        local_stage_path.mkdir(parents=True, exist_ok=True)

        # Heavy I/O: Pull from network
        shutil.unpack_archive(str(zip_path), str(local_stage_path), "zip")

        # Explode Nested (Local I/O)
        for nested_zip in local_stage_path.rglob("*RSM_RawArchive.zip"):
            try:
                shutil.unpack_archive(str(nested_zip), str(nested_zip.parent), "zip")
                nested_zip.unlink()
            except Exception:
                logger.warning(f"Failed to explode nested zip in {zip_path.name}")

        return (folder_id, local_stage_path)

    def _phase_process_batch(self, staged_items: List[tuple], pbar: tqdm):
        """
        Phase 2: Crunch the data on SSD and push to DB.
        """
        # We use fewer threads here because this is CPU/DB bound
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.process_workers) as executor:
            futures = [
                executor.submit(process_staged_directory, self.eng, fid, path, self.ctx)
                for fid, path in staged_items
            ]
            
            # Update progress bar as each PROCESSING task finishes
            for _ in concurrent.futures.as_completed(futures):
                pbar.update(1)

    def _phase_cleanup_batch(self, staged_items: List[tuple]):
        """
        Phase 3: Nuke the staging folders.
        """
        for _, path in staged_items:
            try:
                if path.exists():
                    shutil.rmtree(path)
            except OSError as e:
                logger.warning(f"Failed to cleanup {path}: {e}")