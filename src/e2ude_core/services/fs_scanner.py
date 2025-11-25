from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import os
from tqdm import tqdm

logger = logging.getLogger(__name__)

def _search_dir(search_path):
    """
    Scans a single directory. Returns subdirectories to recurse and relevant files found.
    """
    add_dirs = []
    zips_found = []
    try:
        # os.scandir is context manager, ensures handles are closed
        with os.scandir(search_path) as it:
            for res in it:
                if res.is_dir(follow_symlinks=False):
                    add_dirs.append(res.path)
                # Check extension case-insensitively for Windows safety
                elif res.is_file(follow_symlinks=False) and res.name.lower().endswith(".e2d.zip"):
                    zips_found.append(Path(res.path))
    except (PermissionError, OSError) as e:
        # Debug level to avoid spamming console on common permission issues
        logger.debug(f"Access denied or error: {search_path} [{e}]")
        
    return add_dirs, zips_found

def scan_for_rsm_zips(search_path: Path, max_workers=64):
    if not search_path.exists():
        raise ValueError(f"Search path does not exist: {search_path}")

    logger.info(f"Scanning {search_path} for RSM zips...")
    
    all_zips = []
    
    # Use a set to track active futures
    futures = set()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Seed the pool with the root
        futures.add(executor.submit(_search_dir, str(search_path)))
        
        # Dynamically increase 'total' as we discover subdirectories.
        with tqdm(desc="Scanning Directories", unit="dir", total=1) as pbar:
            while futures:
                # This blocks until at least one task finishes. No busy waiting.
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                
                for f in done:
                    try:
                        dirs, zips = f.result()
                        
                        if dirs:
                            pbar.total += len(dirs)
                            pbar.refresh()  # Force redraw so the percentage doesn't jump weirdly
                        
                        pbar.update(1)
                        
                        if zips:
                            all_zips.extend(zips)
                            pbar.set_postfix(found=len(all_zips))
                        
                        for d in dirs:
                            futures.add(executor.submit(_search_dir, d))
                            
                    except Exception as e:
                        logger.error(f"Scan error: {e}")
                        # Still mark progress even on failure to avoid hanging bar
                        pbar.update(1)

    logger.info(f"Scan complete. Found {len(all_zips)} zips.")
    return all_zips

if __name__ == "__main__":
    # Example usage
    test_path = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
    # Ensure path exists before running to avoid immediate crash in example
    if test_path.exists():
        zips = scan_for_rsm_zips(test_path)
        print(f"First 5 found: {zips[:5]}")
    else:
        print(f"Path not found: {test_path}")