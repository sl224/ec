import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict
from tqdm import tqdm

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.services.discovery import discover_network_zips

# Import State Logic
from e2ude_core.orchestration.state import get_folder_states_bulk, FolderState
from e2ude_core.pipelines.scanner import SCANNER_VERSION

logger = logging.getLogger(__name__)

STAGING_ROOT = Path("D:/E2UDE_STAGING")
if not STAGING_ROOT.exists():
    try:
        STAGING_ROOT = Path("temp_staging")
        STAGING_ROOT.mkdir(exist_ok=True)
    except:
        pass


def filter_folders_bulk(eng, folder_map: Dict[Path, int]) -> Dict[Path, int]:
    """
    Fast batch filtering of folders.
    """
    total = len(folder_map)
    logger.info(f"Checking state for {total} folders (Bulk Mode)...")

    needed = {}
    all_paths = list(folder_map.keys())

    # Chunk size for MSSQL param limits
    # Increased to 5000 to reduce connection overhead.
    # The inner function (get_folder_states_bulk) handles the 2000-param limit internally.
    CHUNK_SIZE = 5000

    with tqdm(total=total, desc="Checking DB State", unit="folder") as pbar:
        for i in range(0, total, CHUNK_SIZE):
            chunk_paths = all_paths[i : i + CHUNK_SIZE]
            chunk_ids = [folder_map[p] for p in chunk_paths]

            try:
                # Single DB round-trip for 5000 folders (internally batched)
                states = get_folder_states_bulk(eng, chunk_ids, SCANNER_VERSION)

                for p, fid in zip(chunk_paths, chunk_ids):
                    state = states.get(fid, FolderState.NEEDS_SCAN)
                    if state != FolderState.UP_TO_DATE:
                        needed[p] = fid

            except Exception as e:
                logger.error(f"Failed batch state check: {e}")
                # If check fails, assume we need to process them
                for p, fid in zip(chunk_paths, chunk_ids):
                    needed[p] = fid

            pbar.update(len(chunk_paths))

    logger.info(f"State Check Complete. {len(needed)} folders require processing.")
    return needed


def main():
    setup_logging(settings)
    logger.info(f"Starting Selective Hybrid Pipeline. Staging: {STAGING_ROOT}")

    # Create a local engine just for the initial setup/scanning
    main_eng = sql_io.get_engine(settings.database, default_pool_size=64)

    try:
        initialize_database(main_eng, reset_tables=False)

        # 1. Discovery
        # scan_root = Path(r"\\esidme24\#ESIDME24\PUBLIC\E2 Stuff\ALE RSM Data Archive")
        scan_root = Path(r"\\Rsiny1-ilsfs\RSM")
        if not scan_root.exists():
            logger.error("Scan root not found.")
            return

        valid_paths = discover_network_zips(scan_root, max_workers=1024)
        if not valid_paths:
            logger.info("No zips found.")
            return

        # 2. Registration
        all_folders_map = register_folders_bulk(main_eng, valid_paths)

        if not all_folders_map:
            logger.info("No folders registered.")
            return

        # 3. Fast Filtering
        workable_map = filter_folders_bulk(main_eng, all_folders_map)

        if not workable_map:
            logger.info("All folders are up to date. Exiting.")
            return

        # 4. Pipeline Execution
        # CHANGED: Pass settings.database instead of main_eng
        pipeline = StagingPipeline(
            db_settings=settings.database,  # Pass the Pydantic model
            folder_id_map=workable_map,
            staging_root=STAGING_ROOT,
            buffer_size=60,
            unzip_workers=60,  # High I/O concurrency
            process_workers=8,  # Matched to CPU cores
            db_write_workers=4,  # Threads inside each process
        )
        pipeline.run()

    except KeyboardInterrupt:
        print("\n[!] Force Quit (Ctrl+C) Detected.")
        logger.warning("Killing all threads...")

        # --- VIZTRACER CONTROL ---
        # Only verify/save if ENABLE_VIZTRACER env var is set
        if os.environ.get("ENABLE_VIZTRACER") == "1":
            try:
                from viztracer import get_tracer

                tracer = get_tracer()
                if tracer:
                    print("Saving VizTracer data...")
                    output_file = f"trace_{int(time.time())}.json"
                    tracer.save(output_file=output_file)
                    print(f"Trace saved to {output_file}")
            except ImportError:
                pass
            except Exception as e:
                print(f"Failed to save trace: {e}")

        # Use sys.exit to allow cleanup hooks
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")


if __name__ == "__main__":
    main()
