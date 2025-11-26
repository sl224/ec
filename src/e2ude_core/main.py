import logging
from pathlib import Path

# --- Core ETL Imports ---
from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
<<<<<<< HEAD
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.services.discovery import discover_network_zips
=======
from e2ude_core.services.fs_scanner import scan_for_rsm_zips
>>>>>>> refs/remotes/origin/fix_folder

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

    logger.info(f"Starting Core Pipeline. Staging: {STAGING_ROOT}")

    # Pool size = Consumers + Producer + Main
    main_eng = sql_io.get_engine(settings.database, default_pool_size=16)

    try:
        initialize_database(main_eng, reset_tables=True)

        # 1. Discovery (Fast Walk)
        # scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        scan_root = Path(r"tests/static_assets/")

        valid_paths = discover_network_zips(scan_root, max_workers=1024)
        if not valid_paths:
            logger.info("No zips found.")
            return

        # 2. Registration
        folder_id_map = register_folders_bulk(main_eng, valid_paths)

        # 3. Execution (Pipeline)
        pipeline = StagingPipeline(
            eng=main_eng,
            zip_paths=valid_paths,
            folder_id_map=folder_id_map,
            staging_root=STAGING_ROOT,
            num_consumers=8,
            queue_size=16,
        )
        pipeline.run()

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")


if __name__ == "__main__":
    main()