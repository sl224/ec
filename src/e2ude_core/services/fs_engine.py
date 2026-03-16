import logging
import os
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Callable, List, TypeVar, Any, Dict
from tqdm import tqdm

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ParallelFileScanner:
    """Parallel directory walker used by discovery and file cataloging."""

    def __init__(self, max_workers: int = 64):
        self.max_workers = max_workers

    def walk(
        self,
        root_path: Path,
        filter_func: Callable[[os.DirEntry], bool],
        action_func: Callable[[Path], T],
        *,
        show_progress: bool = True,
        progress_desc: str = "Scanning Directories",
    ) -> List[T]:
        """Walk `root_path` and apply `action_func` to matching files."""
        futures: Dict[Any, str] = {}
        results: List[T] = []

        dirs_scanned = 0
        files_found = 0

        logger.info(
            f"Starting parallel scan of {root_path} ({self.max_workers} workers)"
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            root_future = executor.submit(self._scan_dir, str(root_path), filter_func)
            futures[root_future] = "SCAN"

            with tqdm(
                total=1,
                desc=progress_desc,
                unit="dir",
                disable=not show_progress,
            ) as pbar:
                while futures:
                    done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)

                    for f in done:
                        task_type = futures.pop(f)

                        try:
                            if task_type == "SCAN":
                                dirs_scanned += 1
                                subdirs, matching_files = f.result()

                                if subdirs:
                                    pbar.total += len(subdirs)
                                    pbar.refresh()

                                for d in subdirs:
                                    nf = executor.submit(self._scan_dir, d, filter_func)
                                    futures[nf] = "SCAN"

                                for p in matching_files:
                                    files_found += 1
                                    nf = executor.submit(action_func, Path(p))
                                    futures[nf] = "ACTION"

                                pbar.update(1)
                                pbar.set_postfix(files=files_found)

                            elif task_type == "ACTION":
                                res = f.result()
                                if res is not None:
                                    results.append(res)

                        except Exception as e:
                            logger.error(f"Task failed: {e}")
                            if task_type == "SCAN":
                                pbar.update(1)

        logger.info(
            f"Scan complete. Scanned {dirs_scanned} dirs, processed {len(results)} files."
        )
        return results

    def _scan_dir(self, path: str, filter_func: Callable[[os.DirEntry], bool]):
        """Return subdirectories and matching files for one directory."""
        subdirs = []
        files = []

        try:
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
