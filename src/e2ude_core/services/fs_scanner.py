# %%
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm import tqdm
from collections import deque

logger = logging.getLogger(__name__)


def _search_dir(search_path):
    add_dirs = []
    zips_found = []
    for res in os.scandir(search_path):
        if res.is_dir(follow_symlinks=False):
            add_dirs.append(res.path)
        elif res.is_file and res.name.endswith(".e2d.zip"):
            zips_found.append(Path(res.path))
    return add_dirs, zips_found


def scan_for_rsm_zips(search_path: Path, top=None):
    if not search_path.exists():
        raise ValueError(f"Search path does not exist: {search_path}")

    if top is None:
        top = float('inf')

    logger.info(f"Scanning {search_path} for RSM zips...")
    zips_found = []
    pbar = tqdm()
    with ThreadPoolExecutor() as executor:
        futures_running = deque([executor.submit(_search_dir, search_path)])
        while futures_running:
            f = futures_running.popleft()
            if f.done():
                dirs_found, zips_found_sub = f.result()
                futures_running.extend(executor.submit(_search_dir, d) for d in dirs_found)
                zips_found.extend(zips_found_sub)
                pbar.update(len(zips_found_sub))
            else:
                print('Still running')
                futures_running.append(f)
    pbar.close()
    logger.info(f"Found {len(zips_found)} zips.")
    return zips_found


if __name__ == "__main__":
    test = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
    zips_found = scan_for_rsm_zips(test)
