import logging
import sys
from pathlib import Path
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sqlalchemy as sa

# --- Core ETL Imports ---
from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_zip
from e2ude_core.db import access as sql_io
from e2ude_core.config import settings
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.services.fs_scanner import scan_for_rsm_zips

logger = logging.getLogger(__name__)


def worker_task(args: Tuple[sa.Engine, int, Path, EtlContext]):
    """
    The entry point for a Thread Worker.
    """
    eng, folder_id, zip_path, context = args

    try:
        # We use the shared engine. process_zip manages its own connections/sessions.
        process_zip(
            eng=eng, folder_id=folder_id, zip_path=zip_path, context=context
        )
    except Exception as e:
        logger.error(f"Worker failed processing {zip_path}: {e}", exc_info=True)


def main():
    """
    Main entry point (Threaded Version).
    """
    # 1. Configure Standard Logging
    setup_logging(settings)
    
    # Global concurrency setting
    max_threads = settings.worker_threads
    
    logger.info(
        f"Starting E2UDE Core. DB: {settings.database.type}, Threads: {max_threads}"
    )

    # 2. Create ONE Shared Engine
    # Pass worker count as default pool size to prevent QueuePool limits
    main_eng = sql_io.get_engine(
        settings.database, 
        default_pool_size=max_threads
    )

    # Define executor outside try/finally so we can reference it in finally if needed
    executor = None

    try:
        # 3. Setup DB
        initialize_database(main_eng, reset_tables=False)

        # 4. Discovery Phase (Incremental Scan)
        # Instead of querying the DB for a fixed list, we scan the network drive
        # and let the discovery service tell us what is NEW or CHANGED.
        # scan_root = Path(r"\\rsiny1-ilsfs\RSM") 
        scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        # scan_root = Path(r"tests/static_assets") 
        
        if not scan_root.exists():
            logger.error(f"Scan root not found: {scan_root}")
            return

        logger.info(f"Scanning {scan_root} for new files...")
        
        # Pass max_threads to the scanner so discovery is also fast
        valid_paths = scan_for_rsm_zips(scan_root, max_workers=1024)

        logger.info(f"Discovery complete. Found {len(valid_paths)} NEW files to process.")

        if not valid_paths:
            logger.info("No new work found. Exiting.")
            return

        ctx = EtlContext.capture()
        work_items = []

        # 5. Bulk Register
        folder_id_map = register_folders_bulk(main_eng, valid_paths)

        # 6. Build Work Args
        for zip_path in valid_paths:
            new_folder_id = folder_id_map.get(zip_path)
            if new_folder_id:
                work_items.append(
                    (
                        main_eng,
                        new_folder_id,
                        zip_path,
                        ctx,
                    )
                )

        if not work_items:
            logger.info("No valid work items prepared.")
            return

        # 7. Process in Parallel Threads
        # I/O Bound task = High thread count is okay.
        logger.info(f"Dispatching {len(work_items)} jobs to {max_threads} threads.")

        # We do NOT use the context manager here because we want manual control over shutdown
        executor = ThreadPoolExecutor(max_workers=max_threads)
        futures = [executor.submit(worker_task, item) for item in work_items]
        
        try:
            # Monitor progress as they complete
            for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing Zips", unit="zip"):
                pass
        except KeyboardInterrupt:
            logger.warning("\n[!] Processing interrupted by user (Ctrl+C). Shutting down...")
            # Cancel all pending futures (python 3.9+)
            executor.shutdown(wait=False, cancel_futures=True)
            raise

        logger.info("All threads finished.")

    except KeyboardInterrupt:
        # Catch the re-raised interrupt to exit cleanly
        logger.info("Exiting.")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Main process terminated unexpectedly: {e}", exc_info=True)
    finally:
        if executor:
            # Ensure any remaining threads are cleaned up
            executor.shutdown(wait=True)
        main_eng.dispose()


if __name__ == "__main__":
    main()