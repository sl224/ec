import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import BoundedSemaphore, Event
from typing import Dict, List, Set
from zipfile import ZipFile

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.services.zip_io import file_type_patterns

# Import State Logic
from e2ude_core.orchestration.state import get_folder_work_delta, FolderState
from e2ude_core.pipelines.scanner import SCANNER_VERSION

# --- VizTracer Import (Safe) ---
try:
    from viztracer import get_tracer
except ImportError:
    def get_tracer(): return None

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


class StagingPipeline:
    """
    High-Throughput Continuous Pipeline (Thread-Based).
    """

    def __init__(
        self,
        eng: sa.Engine,
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        buffer_size: int = 30,
        unzip_workers: int = 32,
        process_workers: int = 8,
        db_write_workers: int = 4,
    ):
        self.eng = eng
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root

        self.unzip_workers = unzip_workers
        self.process_workers = process_workers
        self.db_write_workers = db_write_workers

        self.buffer_sem = BoundedSemaphore(value=buffer_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()
        
        self.active_patterns = _resolve_active_patterns()
        logger.info(f"Active Patterns: {len(self.active_patterns)}")

        self.skipped_count = 0

    def run(self):
        total = len(self.folder_id_map)
        logger.info(f"Starting Pipeline. Scanning {total} candidates...")

        # Log Pipeline Start for VizTracer
        tracer = get_tracer()
        if tracer:
            tracer.log_instant("Pipeline Start", {"total_files": total})

        unzip_pool = ThreadPoolExecutor(max_workers=self.unzip_workers, thread_name_prefix="Stage")
        process_pool = ThreadPoolExecutor(max_workers=self.process_workers, thread_name_prefix="Proc")
        LIMIT = 100000 

        try:
            with tqdm(total=total, desc="Pipeline", unit="zip") as pbar:
                pbar.set_postfix(skipped=0, active=0)
                
                for i, zip_path in enumerate(self.folder_id_map.keys()):
                    if self.stop_event.is_set() or i == LIMIT:
                        break

                    while not self.buffer_sem.acquire(timeout=0.5):
                        if self.stop_event.is_set():
                            break

                    unzip_pool.submit(
                        self._task_stage_selective,
                        zip_path,
                        process_pool,
                        pbar,
                    )

                unzip_pool.shutdown(wait=True)
                process_pool.shutdown(wait=True)

        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted. Force stopping...")
            self.stop_event.set()
            unzip_pool.shutdown(wait=False)
            process_pool.shutdown(wait=False)
            raise

    def _task_stage_selective(
        self, 
        zip_path: Path, 
        process_pool: ThreadPoolExecutor, 
        pbar: tqdm
    ):
        """
        Stage 1: Selective Network Extraction (Thread I/O Bound).
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
            delta = get_folder_work_delta(self.eng, folder_id, SCANNER_VERSION)
            if delta.status == FolderState.UP_TO_DATE:
                self._finalize_item(None, pbar, skipped=True)
                return
        except Exception as e:
            logger.warning(f"State check failed for {folder_id}, forcing retry: {e}")

        # --- Proceed to Extraction ---
        
        # VizTracer: Log Staging Start
        tracer = get_tracer()
        if tracer:
            tracer.log_instant("Staging Start", {"zip": zip_path.name, "id": folder_id}, scope="t")

        safe_name = f"{folder_id}_{zip_path.stem}"
        local_dir = self.staging_root / safe_name
        
        try:
            if local_dir.exists(): shutil.rmtree(local_dir)
            local_dir.mkdir(parents=True, exist_ok=True)

            with ZipFile(zip_path) as zf:
                members = zf.namelist()
                targets = [
                    m for m in members 
                    if _match_any(m, self.active_patterns) or m.endswith("RSM_RawArchive.zip")
                ]
                
                if targets:
                    zf.extractall(local_dir, members=targets)

            for nested_zip in local_dir.rglob("*RSM_RawArchive.zip"):
                try:
                    nested_root = nested_zip.with_suffix("")
                    nested_root.mkdir(exist_ok=True)
                    
                    with ZipFile(nested_zip) as nz:
                        nested_targets = []
                        for name in nz.namelist():
                            if _match_any(nested_root.name + "/" + name, self.active_patterns):
                                nested_targets.append(name)
                        
                        if nested_targets:
                            nz.extractall(nested_root, members=nested_targets)
                finally:
                    nested_zip.unlink()

            process_pool.submit(self._task_process, folder_id, local_dir, pbar)

        except Exception as e:
            logger.error(f"Staging failed for {zip_path}: {e}")
            self._finalize_item(local_dir, pbar, skipped=False)

    def _task_process(self, folder_id: int, stage_path: Path, pbar: tqdm):
        """
        Stage 2: Database Ingest (Thread CPU/DB Bound).
        """
        if self.stop_event.is_set():
            self._finalize_item(stage_path, pbar, skipped=False)
            return

        # VizTracer: Log Processing Start
        tracer = get_tracer()
        if tracer:
            tracer.log_instant("Processing Start", {"id": folder_id}, scope="t")

        try:
            process_staged_directory(
                self.eng,
                folder_id,
                stage_path,
                self.ctx,
                db_workers=self.db_write_workers,
            )
        except Exception as e:
            logger.error(f"Ingest failed for ID {folder_id}: {e}")
        finally:
            self._finalize_item(stage_path, pbar, skipped=False)

    def _finalize_item(self, path, pbar, skipped: bool):
        if path:
            try:
                if path.exists(): shutil.rmtree(path)
            except OSError: pass

        self.buffer_sem.release()
        
        if skipped:
            self.skipped_count += 1
            
            # VizTracer: Log counter update (every 10 to reduce noise)
            if self.skipped_count % 10 == 0:
                tracer = get_tracer()
                if tracer:
                    tracer.log_var("Skipped Zips", self.skipped_count)
                
                pbar.set_postfix(skipped=self.skipped_count)
        
        pbar.update(1)