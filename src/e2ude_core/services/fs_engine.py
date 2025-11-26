import logging
import os
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Callable, List, TypeVar, Any, Set, Dict

logger = logging.getLogger(__name__)

T = TypeVar("T")

class ParallelFileScanner:
    """
    High-Performance Filesystem Walker.
    
    Architecture:
    - Uses a single ThreadPoolExecutor for both Traversal (IO) and Processing (CPU/IO).
    - Dynamic Work Injection: Discovering a directory immediately schedules a new scan task.
    - Backpressure: Implicitly managed by the ThreadPoolExecutor's queue.
    """

    def __init__(self, max_workers: int = 64):
        self.max_workers = max_workers

    def walk(
        self,
        root_path: Path,
        filter_func: Callable[[os.DirEntry], bool],
        action_func: Callable[[Path], T],
    ) -> List[T]:
        """
        Recursively scans root_path.
        
        Args:
            filter_func: Predicate(os.DirEntry) -> bool.
            action_func: Function(Path) -> T. Executed on matching files.
        """
        # Track all active futures and their type: "SCAN" (Dir) or "ACTION" (File)
        # Mapping: Future -> str
        futures: Dict[Any, str] = {}
        results: List[T] = []
        
        # Metrics for heartbeat logging
        dirs_scanned = 0
        files_found = 0

        logger.info(f"Starting parallel scan of {root_path} ({self.max_workers} workers)")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Bootstrapping: Schedule the root directory
            root_future = executor.submit(self._scan_dir, str(root_path), filter_func)
            futures[root_future] = "SCAN"

            # The Event Loop
            while futures:
                # Wait for at least one task to finish.
                # This prevents busy-waiting and handles completions as they stream in.
                done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)

                for f in done:
                    task_type = futures.pop(f)

                    try:
                        if task_type == "SCAN":
                            dirs_scanned += 1
                            subdirs, matching_files = f.result()

                            # 1. Fan-Out: Schedule Subdirectories
                            for d in subdirs:
                                nf = executor.submit(self._scan_dir, d, filter_func)
                                futures[nf] = "SCAN"

                            # 2. Schedule Actions (Processing)
                            # We submit these to the pool to parallelize heavy work (hashing)
                            # or handle high-latency I/O (network reads).
                            for p in matching_files:
                                files_found += 1
                                nf = executor.submit(action_func, Path(p))
                                futures[nf] = "ACTION"
                            
                            # Periodic Heartbeat (every ~1000 dirs to avoid log spam)
                            if dirs_scanned % 1000 == 0:
                                logger.info(f"Scanned {dirs_scanned} dirs, found {files_found} files...")

                        elif task_type == "ACTION":
                            # Collect Result
                            res = f.result()
                            if res is not None:
                                results.append(res)

                    except Exception as e:
                        # Log but don't crash the scan
                        logger.error(f"Task failed: {e}")

        logger.info(f"Scan complete. Scanned {dirs_scanned} dirs, processed {len(results)} files.")
        return results

    def _scan_dir(self, path: str, filter_func: Callable[[os.DirEntry], bool]):
        """
        Unit of Work: Scans a single directory.
        Returns: (list_of_subdir_paths, list_of_file_paths)
        """
        subdirs = []
        files = []

        try:
            # os.scandir is strictly better than os.walk or pathlib.iterdir
            # because it yields DirEntry objects with cached stat() info.
            with os.scandir(path) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append(entry.path)
                    elif entry.is_file(follow_symlinks=False):
                        if filter_func(entry):
                            files.append(entry.path)
                            
        except (PermissionError, OSError) as e:
            logger.debug(f"Access denied or error: {path} [{e}]")

        return subdirs, files