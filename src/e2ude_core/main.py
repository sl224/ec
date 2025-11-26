import logging
import os
import sys
from pathlib import Path
from typing import Dict

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
        # We register ALL folders so we have IDs to query history against.
        all_folders_map = register_folders_bulk(main_eng, valid_paths)

        if not all_folders_map:
            logger.info("No folders registered.")
            return

        # 3. Pipeline Execution
        # We pass the FULL list. The pipeline will check `get_folder_work_delta`
        # for each item just before processing, skipping active work if not needed.
        pipeline = StagingPipeline(
            eng=main_eng,
            folder_id_map=all_folders_map,
            staging_root=STAGING_ROOT,
            buffer_size=60,
            unzip_workers=60,
            process_workers=8,
            db_write_workers=8
        )
        pipeline.run()

    except KeyboardInterrupt:
        logger.warning("\n[!] Force Quit (Ctrl+C). Killing all threads immediately...")
        try:
            from viztracer import get_tracer
            tracer = get_tracer()
            if tracer:
                logger.info("VizTracer active. Saving trace data...")
                tracer.stop()
                tracer.save()
        except ImportError: pass
        except Exception: pass
        os._exit(1)

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")

if __name__ == "__main__":
    main()