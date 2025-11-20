import logging
import multiprocessing
from pathlib import Path
from typing import Tuple, List, Any

# --- Core ETL Imports ---
from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_zip
from e2ude_core.db import access as sql_io
from e2ude_core.config import settings
from e2ude_core.db.setup import (
    initialize_database,
    register_folders_bulk,
)  # Changed import
from e2ude_core.logging_mp import listener_process, worker_configurer

# Note: In 'spawn' mode, the global logger must be retrieved inside functions,
# but we can define a placeholder here.
logger = logging.getLogger(__name__)


def get_data(eng) -> List[Tuple[int, Any]]:
    """
    Fetches list of (FolderID, FolderPath).
    Using provided test data configuration.
    """
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


def worker_task(args: Tuple[Any, str, int, Path, EtlContext]):
    """
    The entry point for a Worker Process.
    It sets up its own environment (logging, DB) and processes ONE item.
    """
    log_queue, log_level, folder_id, zip_path, context = args

    # 1. Configure Logging for this specific worker process
    worker_configurer(log_queue, log_level)

    # 2. Create a FRESH Database Engine
    try:
        worker_eng = sql_io.get_engine(settings.database)

        # 3. Run the Workflow
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
    # 1. Enforce 'spawn' for safety with SQLAlchemy/ODBC
    multiprocessing.set_start_method("spawn", force=True)

    # 2. Setup Centralized Logging Queue via Manager
    manager = multiprocessing.Manager()

    try:
        log_queue = manager.Queue(-1)

        # Configuration for the listener process
        listener_config = {
            "log_file": settings.logging.log_file,
            "level": settings.logging.level,
            "rotation_size_mb": settings.logging.rotation_size_mb,
            "rotation_backup_count": settings.logging.rotation_backup_count,
            "format": settings.logging.format,
        }

        # Start the Listener
        listener = multiprocessing.Process(
            target=listener_process, args=(log_queue, listener_config)
        )
        listener.start()

        # Configure the Main Process to also log to the queue
        worker_configurer(log_queue, settings.logging.level)

        # --- Main Logic ---
        pool = None
        try:
            logger.info(
                f"Starting E2UDE Core [Multiprocessing]. DB: {settings.database.type}"
            )

            # 3. Initialize Database (One-time setup)
            main_eng = sql_io.get_engine(settings.database)
            # WARNING: reset_tables=True wipes data. Use False for production.
            initialize_database(main_eng, reset_tables=True)

            # 4. Fetch Work
            try:
                source_data = get_data(main_eng)
            except Exception as e:
                logger.warning(f"Could not fetch data: {e}")
                source_data = []

            logger.info(f"Found {len(source_data)} potential folders to process.")

            if not source_data:
                logger.warning("No work found. Exiting.")
            else:
                # 5. Context & Pre-processing
                ctx = EtlContext.capture()
                work_items = []

                logger.info("Pre-registering folders in database...")

                # Filter valid paths first
                valid_paths = []
                for _, zip_path_obj in source_data:
                    p = Path(zip_path_obj)
                    if p.exists():
                        valid_paths.append(p)
                    else:
                        logger.warning(f"Skipping missing file: {p}")

                # Bulk Register (Optimized)
                # Returns {Path: folder_id}
                folder_id_map = register_folders_bulk(main_eng, valid_paths)

                # Build work items from the map
                for zip_path in valid_paths:
                    # Get the ID (it might be missing if registration failed for some reason, e.g. bad name)
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

                # 6. Launch Multiprocessing Pool
                if not work_items:
                    logger.info("No valid work items prepared.")
                else:
                    cpu_count = multiprocessing.cpu_count()
                    num_workers = max(1, cpu_count - 2)
                    num_workers = 1  # Forced for debugging/safety

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
            logger.debug(
                "\n[Manual Interrupt] Interrupted by user. Terminating workers..."
            )
            if pool:
                pool.terminate()
                pool.join()
            logger.warning("Processing interrupted by user.")

        except Exception as e:
            logger.critical(f"Main process terminated unexpectedly: {e}", exc_info=True)
            import traceback

            traceback.print_exc()
        finally:
            # 7. Cleanup Logging
            logger.info("Shutting down logging listener...")
            log_queue.put(None)
            listener.join()

    finally:
        manager.shutdown()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
