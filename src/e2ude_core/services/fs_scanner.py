# %%
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm import tqdm

logger = logging.getLogger(__name__)


def _search_dir(search_path: Path, zips_found, top):
    add_dirs = []
    for res in os.scandir(search_path):
        if res.is_dir(follow_symlinks=False):
            add_dirs.append(res.path)
        elif res.is_file and res.name.endswith(".e2d.zip"):
            zips_found.append(Path(res.path))
            if len(zips_found) == top:
                break
    return add_dirs


def scan_for_rsm_zips(search_path: Path, top=None):
    if not search_path.exists():
        raise ValueError(f"Search path does not exist: {search_path}")

    if top is None:
        top = float("inf")

    logger.info(f"Scanning {search_path} for RSM zips...")
    search_dirs = [search_path]
    zips_found = []
    futures = []
    pbar = tqdm()
    with ThreadPoolExecutor() as executor:
        while search_dirs and len(zips_found) < top:
            while search_dirs:
                future = executor.submit(
                    _search_dir, search_dirs.pop(), zips_found, top
                )
                futures.append(future)
            while len(zips_found) < top and futures:
                prior = len(zips_found)
                search_dirs.extend(futures.pop().result())
                new_zips_found_count = len(zips_found) - prior + 1
                pbar.update(new_zips_found_count)
    pbar.close()
    logger.info(f"Found {len(zips_found)} zips.")
    return zips_found


if __name__ == "__main__":
    test = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive\166502")
    zips_found = scan_for_rsm_zips(test)
