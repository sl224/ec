import logging
from pathlib import Path

# --- Core ETL Imports ---
from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.services.discovery import discover_network_zips

logger = logging.getLogger(__name__)

# --- Configuration ---
STAGING_ROOT = Path("D:/E2UDE_STAGING")
if not STAGING_ROOT.exists():
    try:
        STAGING_ROOT = Path("temp_staging")
        STAGING_ROOT.mkdir(exist_ok=True)
    except:
        pass

def main():
    setup_logging(settings)

    logger.info(f"Starting Batch Pipeline. Staging: {STAGING_ROOT}")

    # Pool size: Needs to support the Process Phase workers (8) + Main thread
    main_eng = sql_io.get_engine(settings.database, default_pool_size=16)

    try:
        initialize_database(main_eng, reset_tables=False)

        # 1. Discovery
        scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        scan_root = Path(r"tests/static_assets")
        valid_paths = discover_network_zips(scan_root, max_workers=1024)
        
        if not valid_paths:
            logger.info("No zips found.")
            return

        # 2. Registration
        folder_id_map = register_folders_bulk(main_eng, valid_paths)

        # 3. Execution
        # Batch Size 30: Good balance between network saturation and disk usage
        # Unzip Workers 30: 1 thread per file in the batch -> Maximize parallel download
        pipeline = StagingPipeline(
            eng=main_eng,
            zip_paths=valid_paths,
            folder_id_map=folder_id_map,
            staging_root=STAGING_ROOT,
            batch_size=30,
            unzip_workers=30, 
            process_workers=8 
        )
        pipeline.run()

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")

if __name__ == "__main__":
    main()