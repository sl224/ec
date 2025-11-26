import logging
import os
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.services.discovery import discover_network_zips

# New imports for filtering
from e2ude_core.orchestration.state import get_folder_work_delta, FolderState
from e2ude_core.pipelines.scanner import SCANNER_VERSION

logger = logging.getLogger(__name__)

STAGING_ROOT = Path("D:/E2UDE_STAGING")
if not STAGING_ROOT.exists():
    try:
        STAGING_ROOT = Path("temp_staging")
        STAGING_ROOT.mkdir(exist_ok=True)
    except: pass

def filter_folders_needing_work(eng, folder_map: Dict[Path, int]) -> Dict[Path, int]:
    """
    Pre-flight check: Checks DB state to see which folders actually need work.
    Filters out UP_TO_DATE folders to avoid unnecessary IO in the pipeline.
    """
    needed = {}
    logger.info(f"Checking state for {len(folder_map)} folders...")
    
    # Use threads to check DB state in parallel (IO bound mostly)
    with ThreadPoolExecutor(max_workers=16) as executor:
        # Map future -> (path, id)
        future_map = {
            executor.submit(get_folder_work_delta, eng, fid, SCANNER_VERSION): (path, fid)
            for path, fid in folder_map.items()
        }
        
        for f in future_map:
            path, fid = future_map[f]
            try:
                delta = f.result()
                if delta.status != FolderState.UP_TO_DATE:
                    needed[path] = fid
            except Exception as e:
                logger.warning(f"Failed to check state for {fid}: {e}")
                # Assume needed if check fails? Or skip? Skipping is safer.
    
    logger.info(f"State Check Complete. {len(needed)} folders require processing.")
    return needed

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
        all_folders_map = register_folders_bulk(main_eng, valid_paths)

        # 3. Smart Filtering
        # Filter the map so we only process new data or data needing updates
        workable_map = filter_folders_needing_work(main_eng, all_folders_map)

        if not workable_map:
            logger.info("All folders are up to date.")
            return

        # 4. Pipeline Execution
        pipeline = StagingPipeline(
            eng=main_eng,
            folder_id_map=workable_map,
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
        except ImportError:
            pass
        except Exception:
            pass
        os._exit(1)

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")

if __name__ == "__main__":
    main()