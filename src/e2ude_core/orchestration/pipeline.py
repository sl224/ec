import logging
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List, Set
from zipfile import ZipFile

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.services.zip_io import FILE_PATTERNS, FileType

logger = logging.getLogger(__name__)


def _resolve_active_patterns() -> List[str]:
    """
    Cross-references the Handler Registry with File Patterns.
    Returns a list of glob patterns for files we actually know how to parse.
    """
    active_types: Set[str] = set(HANDLER_REGISTRY.keys())
    patterns = []

    for f_enum, pattern in FILE_PATTERNS:
        # Convert Enum to string value to match Registry keys
        if f_enum.value in active_types:
            patterns.append(pattern)

    # Always include the nested archive container itself so we can inspect it
    patterns.append("*RSM_RawArchive.zip")
    return patterns


def _match_any(path_str: str, patterns: List[str]) -> bool:
    """Helper for checking if a path matches any of our active globs."""
    p = Path(path_str)
    for pat in patterns:
        if p.match(pat):
            return True
    return False


def _worker_unzip_selective(
    zip_source: Path, extract_dest: Path, patterns: List[str]
) -> bool:
    """
    Smart Extraction Worker (Runs in Process).
    1. Scans Outer Zip headers.
    2. Extracts ONLY files matching `patterns`.
    3. If a nested `RSM_RawArchive.zip` is found, extracts it,
       scans ITs headers, extracts its relevant matches, then deletes it.
    """
    try:
        # 1. Outer Zip
        with ZipFile(zip_source) as zf:
            outer_members = zf.namelist()
            
            # Identify interesting files
            # We always extract the nested archive container to process it
            targets = [
                m for m in outer_members 
                if _match_any(m, patterns) or m.endswith("RSM_RawArchive.zip")
            ]
            
            if targets:
                zf.extractall(extract_dest, members=targets)

        # 2. Handle Nested Archives (The "Drill Down")
        # We look for the zip we just extracted
        for nested_zip_path in extract_dest.rglob("*RSM_RawArchive.zip"):
            try:
                # We extract into a folder named after the zip to preserve structure
                # e.g. "123_RSM_RawArchive.zip" -> folder "123_RSM_RawArchive/"
                # This ensures patterns like "*_RSM_RawArchive/RSM/..." still match.
                nested_root = nested_zip_path.with_suffix("")
                nested_root.mkdir(exist_ok=True)

                with ZipFile(nested_zip_path) as nz:
                    # Virtual Match: Check if "ContainerDir/InnerFile" matches pattern
                    nested_targets = []
                    for name in nz.namelist():
                        # Simulate the full path that would exist on disk
                        virtual_path = nested_root.name + "/" + name
                        if _match_any(virtual_path, patterns):
                            nested_targets.append(name)
                    
                    if nested_targets:
                        nz.extractall(nested_root, members=nested_targets)

            finally:
                # Crucial: Delete the nested zip immediately to save space
                nested_zip_path.unlink()

        # 3. Delete Source Zip
        os.remove(zip_source)
        return True

    except Exception as e:
        # Clean up mess on failure
        if extract_dest.exists():
            shutil.rmtree(extract_dest, ignore_errors=True)
        raise e


class StagingPipeline:
    """
    High-Throughput 3-Stage Pipeline with Selective Extraction.
    """

    def __init__(
        self,
        eng: sa.Engine,
        zip_paths: List[Path],
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,
        download_workers: int = 32,
        unzip_workers: int = 8,
        db_workers: int = 8,
        table_write_workers: int = 4,
    ):
        self.eng = eng
        self.zip_paths = zip_paths
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root

        self.download_workers = download_workers
        self.unzip_workers = unzip_workers
        self.db_workers = db_workers
        self.table_write_workers = table_write_workers

        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()
        
        # Pre-compute patterns once
        self.active_patterns = _resolve_active_patterns()
        logger.info(f"Pipeline active patterns: {self.active_patterns}")

    def run(self):
        total = len(self.zip_paths)
        logger.info(f"Starting 3-Stage Pipeline. Processing {total} files.")

        down_pool = ThreadPoolExecutor(
            max_workers=self.download_workers, thread_name_prefix="Net"
        )
        unzip_pool = ProcessPoolExecutor(max_workers=self.unzip_workers)
        db_pool = ThreadPoolExecutor(
            max_workers=self.db_workers, thread_name_prefix="DB"
        )

        try:
            with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                futures = []

                for zip_path in self.zip_paths:
                    if self.stop_event.is_set():
                        break

                    self.buffer_sem.acquire()

                    f = down_pool.submit(
                        self._task_download,
                        zip_path,
                        unzip_pool,
                        db_pool,
                        pbar,
                    )
                    futures.append(f)

                down_pool.shutdown(wait=True)
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
        if self.stop_event.is_set():
            self._finalize_item(None, pbar)
            return

        folder_id = self.folder_id_map.get(zip_path)
        if not folder_id:
            self._finalize_item(None, pbar)
            return

        safe_name = f"{folder_id}_{zip_path.stem}"
        local_dir = self.staging_root / safe_name
        local_zip = self.staging_root / f"{safe_name}.temp_zip"

        try:
            if local_dir.exists():
                shutil.rmtree(local_dir)
            if local_zip.exists():
                os.remove(local_zip)
            local_dir.mkdir(parents=True, exist_ok=True)

            # 1. Network Copy
            shutil.copy2(str(zip_path), str(local_zip))

            # 2. Selective Unzip (Process)
            # We pass the patterns explicitly to the worker
            future = unzip_pool.submit(
                _worker_unzip_selective, 
                local_zip, 
                local_dir, 
                self.active_patterns
            )

            future.add_done_callback(
                lambda f: self._on_unzip_complete(
                    f, folder_id, local_dir, db_pool, pbar
                )
            )

        except Exception as e:
            logger.error(f"Download failed for {zip_path}: {e}")
            if local_zip.exists():
                os.remove(local_zip)
            self._finalize_item(local_dir, pbar)

    def _on_unzip_complete(self, future, folder_id, local_dir, db_pool, pbar):
        try:
            future.result()  # Check for unzip errors
            db_pool.submit(self._task_ingest, folder_id, local_dir, pbar)
        except Exception as e:
            logger.error(f"Unzip failed for ID {folder_id}: {e}")
            self._finalize_item(local_dir, pbar)

    def _task_ingest(self, folder_id, local_dir, pbar):
        if self.stop_event.is_set():
            self._finalize_item(local_dir, pbar)
            return

        try:
            process_staged_directory(
                self.eng,
                folder_id,
                local_dir,
                self.ctx,
                db_workers=self.table_write_workers,
            )
        except Exception as e:
            logger.error(f"Ingest failed for ID {folder_id}: {e}")
        finally:
            self._finalize_item(local_dir, pbar)

    def _finalize_item(self, path, pbar):
        if path:
            try:
                if path.exists():
                    shutil.rmtree(path)
            except OSError:
                pass

        self.buffer_sem.release()
        pbar.update(1)