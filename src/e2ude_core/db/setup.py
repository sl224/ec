import logging
import multiprocessing
from pathlib import Path
from typing import Tuple, List, Any
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# --- Core ETL Imports ---
from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_zip
from e2ude_core.db import access as sql_io
from e2ude_core.config import settings
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_mp import listener_process, worker_configurer

# Note: In 'spawn' mode, the global logger must be retrieved inside functions,
# but we can define a placeholder here.
logger = logging.getLogger(__name__)


def get_data(eng) -> List[Tuple[int, Any]]:
    """
    Fetches list of (FolderID, FolderPath).
    Using provided test data configuration.
    """
    # ... (Your existing get_data logic) ...
    # Placeholder for the 24k items you seemingly have
    id_paths = [
        (
            0,
            Path(
                r"tests/static_assets/zips/166501_20240212_185419_000_TransportRSM.fpkg.e2d.zip"
            ),
        ),
        (
            1,
            Path(
                r"tests/static_assets/zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
            ),
        ),
    ]
    return id_paths


def check_path_exists(path_obj: Path) -> Path | None:
    """Helper for parallel existence check."""
    return path_obj if path_obj.exists() else None


def worker_task(args: Tuple[Any, str, int, Path, EtlContext]):
    """
    The entry point for a Worker Process.
    """
    log_queue, log_level, folder_id, zip_path, context = args

    worker_configurer(log_queue, log_level)

    try:
        worker_eng = sql_io.get_engine(settings.database)
        process_zip(
            eng=worker_eng, folder_id=folder_id, zip_path=zip_path, context=context
        )
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"Worker crashed processing {zip_path}: {e}", exc_info=True)
    finally:
        if "worker_eng" in locals():
            worker_eng.dispose()


def main():
    """
    Main entry point.
    """
    multiprocessing.set_start_method("spawn", force=True)
    manager = multiprocessing.Manager()

    try:
        log_queue = manager.Queue(-1)

        listener_config = {
            "log_file": settings.logging.log_file,
            "level": settings.logging.level,
            "rotation_size_mb": settings.logging.rotation_size_mb,
            "rotation_backup_count": settings.logging.rotation_backup_count,
            "format": settings.logging.format,
        }

        listener = multiprocessing.Process(
            target=listener_process, args=(log_queue, listener_config)
        )
        listener.start()

        worker_configurer(log_queue, settings.logging.level)

        # --- Main Logic ---
        pool = None
        try:
            logger.info(
                f"Starting E2UDE Core [Multiprocessing]. DB: {settings.database.type}"
            )

            main_eng = sql_io.get_engine(settings.database)
            initialize_database(main_eng, reset_tables=True)

            try:
                # This returns (id, Path) tuples
                source_data = get_data(main_eng)
            except Exception as e:
                logger.warning(f"Could not fetch data: {e}")
                source_data = []

            logger.info(f"Found {len(source_data)} potential folders to process.")

            if not source_data:
                logger.warning("No work found. Exiting.")
            else:
                ctx = EtlContext.capture()
                work_items = []

                logger.info("Verifying file existence (Parallel)...")
                
                # --- OPTIMIZATION: Parallel Existence Check ---
                # Extract just the Path objects for checking
                all_paths = [Path(item[1]) for item in source_data]
                valid_paths = []

                # Use ThreadPool for IO-bound existence checks
                # max_workers=32 is usually a sweet spot for network drives
                with ThreadPoolExecutor(max_workers=32) as executor:
                    # tqdm gives you a progress bar so you know it's not frozen
                    results = list(
                        tqdm(
                            executor.map(check_path_exists, all_paths),
                            total=len(all_paths),
                            desc="Checking Files",
                            unit="file"
                        )
                    )

                # Filter out None results
                for i, res in enumerate(results):
                    if res:
                        valid_paths.append(res)
                    else:
                        # Log missing files (optional: keep concise if many missing)
                        logger.warning(f"Skipping missing file: {all_paths[i]}")

                logger.info(f"Verified {len(valid_paths)} files exist. Registering...")

                # Bulk Register (Optimized)
                folder_id_map = register_folders_bulk(main_eng, valid_paths)

                # Build work items from the map
                for zip_path in valid_paths:
                    new_folder_id = folder_id_map.get(zip_path)
                    if new_folder_id:
                        work_items.append(
                            (
                                log_queue,
                                settings.logging.level,
                                new_folder_id,
                                zip_path,
                                ctx,
                            )
                        )

                main_eng.dispose()

                if not work_items:
                    logger.info("No valid work items prepared.")
                else:
                    cpu_count = multiprocessing.cpu_count()
                    num_workers = max(1, cpu_count - 2)
                    # num_workers = 1 # Uncomment for debugging

                    logger.info(
                        f"Dispatching {len(work_items)} jobs to {num_workers} workers."
                    )

                    pool = multiprocessing.Pool(processes=num_workers)
                    result = pool.map_async(worker_task, work_items)
                    result.get(timeout=None)

                    pool.close()
                    pool.join()

                    logger.info("All workers finished.")

        except KeyboardInterrupt:
            logger.debug("\n[Manual Interrupt] Terminating workers...")
            if pool:
                pool.terminate()
                pool.join()
            logger.warning("Processing interrupted by user.")

        except Exception as e:
            logger.critical(f"Main process terminated unexpectedly: {e}", exc_info=True)
            import traceback
            traceback.print_exc()
        finally:
            logger.info("Shutting down logging listener...")
            log_queue.put(None)
            listener.join()

    finally:
        manager.shutdown()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()