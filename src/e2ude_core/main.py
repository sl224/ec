import logging
import sys
import os
from pathlib import Path
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sqlalchemy as sa
import shutil
import tempfile

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

    executor = None

    try:
        # 3. Setup DB
        initialize_database(main_eng, reset_tables=False)

        # 4. Discovery Phase (Incremental Scan)
        scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        
        if not scan_root.exists():
            logger.error(f"Scan root not found: {scan_root}")
            return

        logger.info(f"Scanning {scan_root} for new files...")
        
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
        logger.info(f"Dispatching {len(work_items)} jobs to {max_threads} threads.")

        executor = ThreadPoolExecutor(max_workers=max_threads)
        futures = [executor.submit(worker_task, item) for item in work_items]
        
        try:
            # Monitor progress
            for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing Zips", unit="zip"):
                pass
        except KeyboardInterrupt:
            # Inner catch to ensure we hit the main Exception block logic
            # or just re-raise to hit the outer block
            raise

        logger.info("All threads finished.")

    except KeyboardInterrupt:
        logger.warning("\n[!] Force Quit (Ctrl+C). Killing all threads immediately...")
        
        # --- VIZTRACER SAFETY CATCH ---
        try:
            # We only import if needed to avoid hard dependency
            from viztracer import get_tracer
            tracer = get_tracer()
            if tracer:
                logger.info("VizTracer active. Saving trace data before exit (this may take a moment)...")
                tracer.stop()
                tracer.save() # Saves to default file (e.g. result.json) or CLI output arg
                logger.info("Trace saved successfully.")
        except ImportError:
            pass # Viztracer not installed/used
        except Exception as e:
            logger.error(f"Failed to save VizTracer data: {e}")
        # -----------------------------

        # Nuclear option: Force kill the process. 
        # This bypasses cleanup handlers (finally blocks), ensuring instant exit.
        os._exit(1)

    except Exception as e:
        logger.critical(f"Main process terminated unexpectedly: {e}", exc_info=True)
    finally:
        # This block is NOT executed if os._exit() is called above.
        if executor:
            executor.shutdown(wait=True)
        main_eng.dispose()


if __name__ == "__main__":
    main()