import logging
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

    # Ensure DB pool is large enough for 32+ concurrent connections
    main_eng = sql_io.get_engine(settings.database, default_pool_size=64)

    try:
        initialize_database(main_eng, reset_tables=False)

        # 1. Discovery
        scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        if not scan_root.exists():
            logger.error("Scan root not found.")
            return

        valid_paths = discover_network_zips(scan_root, max_workers=1024)
        if not valid_paths: return

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

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")

if __name__ == "__main__":
    main()