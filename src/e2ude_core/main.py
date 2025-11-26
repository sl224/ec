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
# Ideally, this should be in settings, but hardcoded for now as per previous iterations
STAGING_ROOT = Path("D:/E2UDE_STAGING")
if not STAGING_ROOT.exists():
    try:
        STAGING_ROOT = Path("temp_staging")
        STAGING_ROOT.mkdir(exist_ok=True)
    except Exception:
        pass


def main():
    setup_logging(settings)

    logger.info(f"Starting Continuous Pipeline. Staging: {STAGING_ROOT}")

    # Pool Size Calculation:
    # We have 8 Process Workers * 4 DB Writers = 32 potential concurrent connections.
    # Plus overhead for the main thread and producer.
    # Setting default_pool_size=64 ensures we never starve for connections during high throughput.
    main_eng = sql_io.get_engine(settings.database, default_pool_size=64)

    try:
        # 0. DB Init
        initialize_database(main_eng, reset_tables=False)

        # 1. Discovery (Fast Walk)
        # Uses the high-concurrency scanner to mask SMB latency
        scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        
        if not scan_root.exists():
            logger.error("Scan root not found.")
            return

        valid_paths = discover_network_zips(scan_root, max_workers=1024)
        
        if not valid_paths:
            logger.info("No zips found.")
            return

        # 2. Registration (Bulk Insert)
        # Registers new folders in the DB and gets their IDs
        folder_id_map = register_folders_bulk(main_eng, valid_paths)

        # 3. Execution (Continuous Pipeline)
        # This triggers the Producer/Consumer flow with the Semaphore "Ticket System"
        pipeline = StagingPipeline(
            eng=main_eng,
            zip_paths=valid_paths,
            folder_id_map=folder_id_map,
            staging_root=STAGING_ROOT,
            buffer_size=30,      # Max 30 folders on SSD (Backpressure)
            network_workers=32,  # High concurrency for Network Unzipping
            process_workers=8,   # CPU concurrency for Parsing
            db_write_workers=4   # Parallel Table Uploads per File
        )
        pipeline.run()

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")


if __name__ == "__main__":
    main()