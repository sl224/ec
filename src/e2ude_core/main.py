import logging
import os
import sys
from pathlib import Path

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.services.discovery import discover_network_zips

logger = logging.getLogger(__name__)

STAGING_ROOT = Path("D:/E2UDE_STAGING")
if not STAGING_ROOT.exists():
    try:
        STAGING_ROOT = Path("temp_staging")
        STAGING_ROOT.mkdir(exist_ok=True)
    except: pass

def main():
    setup_logging(settings)
    logger.info(f"Starting Selective Thread Pipeline. Staging: {STAGING_ROOT}")

    # Ensure DB pool is large enough for concurrent connections
    # 8 process workers * 8 db write workers = 64 connections max burst
    main_eng = sql_io.get_engine(settings.database, default_pool_size=64)

    try:
        initialize_database(main_eng, reset_tables=False)

        # 1. Discovery
        scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        if not scan_root.exists():
            logger.error("Scan root not found.")
            return

        valid_paths = discover_network_zips(scan_root, max_workers=1024)
        if not valid_paths: 
            logger.info("No zips found.")
            return

        # 2. Registration
        folder_id_map = register_folders_bulk(main_eng, valid_paths)

        # 3. Pipeline Execution
        pipeline = StagingPipeline(
            eng=main_eng,
            zip_paths=valid_paths,
            folder_id_map=folder_id_map,
            staging_root=STAGING_ROOT,
            # Tuning for "Selective Extraction" on 8-Core Machine:
            buffer_size=60,
            unzip_workers=60,   # Flood Network with header requests
            process_workers=8,  # Maximize CPU usage for parsing
            db_write_workers=8  # parallel inserts
        )
        pipeline.run()

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
        # This bypasses cleanup handlers (finally blocks) which hang on thread joins.
        os._exit(1)

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        # Note: This block is skipped if os._exit(1) is called above.
        main_eng.dispose()
        logger.info("Exiting.")

if __name__ == "__main__":
    main()