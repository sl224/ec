import logging
import multiprocessing
import gc
import warnings
from pathlib import Path
from typing import Tuple, List, Any

# --- Core ETL Imports ---
from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_zip
from e2ude_core.db import access as sql_io
from e2ude_core.config import settings
from e2ude_core.db.setup import initialize_database, get_or_create_folder
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
    # This redirects all logging.info() calls to the Queue.
    worker_configurer(log_queue, log_level)

    # 2. Create a FRESH Database Engine
    # Engines cannot be shared across processes.
    try:
        worker_eng = sql_io.get_engine(settings.database)

        # 3. Run the Workflow
        process_zip(
            eng=worker_eng, folder_id=folder_id, zip_path=zip_path, context=context
        )
    except Exception as e:
        # Catch-all to ensure the worker doesn't crash silently
        # We use the root logger (via worker_configurer) which sends to queue
        logging.error(f"Worker crashed processing {zip_path}: {e}", exc_info=True)
    finally:
        # 4. Clean up DB resources explicitly
        if "worker_eng" in locals():
            worker_eng.dispose()


def main():
    """
    Main entry point.
    """
    # 1. Enforce 'spawn' for safety with SQLAlchemy/ODBC
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass  # Already set

    # Filter the noisy resource_tracker warning that occurs on shutdown
    # with multiprocessing.Manager queues on macOS/Linux.
    warnings.filterwarnings(
        "ignore", category=UserWarning, module="multiprocessing.resource_tracker"
    )

    # 2. Setup Centralized Logging Queue via Manager
    # Manual manager control to ensure clean shutdown order
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

        # Start the Listener (The only process that writes to the file)
        listener = multiprocessing.Process(
            target=listener_process, args=(log_queue, listener_config)
        )
        listener.start()

        # Configure the Main Process to also log to the queue
        worker_configurer(log_queue, settings.logging.level)

        # --- Main Logic ---
        try:
            logger.info(
                f"Starting E2UDE Core [Multiprocessing]. DB: {settings.database.type}"
            )

            # 3. Initialize Database (One-time setup)
            # We use a temporary engine for the main process setup
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
                for _, zip_path_obj in source_data:
                    # Ensure it's a Path object (your get_data returns Path, but DB returns strings)
                    zip_path = Path(zip_path_obj)

                    if not zip_path.exists():
                        logger.warning(f"Skipping missing file: {zip_path}")
                        continue

                    # We create the folder metadata sequentially here.
                    # This prevents database locking issues if multiple workers
                    # tried to insert the same folder at the same time.
                    new_folder_id = get_or_create_folder(main_eng, zip_path)

                    if new_folder_id:
                        # Pack arguments for the worker
                        work_items.append(
                            (
                                log_queue,
                                settings.logging.level,
                                new_folder_id,
                                zip_path,
                                ctx,
                            )
                        )

                # Close main engine before forking workers to be safe
                main_eng.dispose()

                # 6. Launch Multiprocessing Pool
                if not work_items:
                    logger.info("No valid work items prepared.")
                else:
                    # Leave 2 CPUs free (1 for OS, 1 for Log Listener/Main)
                    cpu_count = multiprocessing.cpu_count()
                    num_workers = max(1, cpu_count - 4)

                    logger.info(
                        f"Dispatching {len(work_items)} jobs to {num_workers} workers."
                    )

                    # Use map (blocks until all done) or imap (lazy)
                    with multiprocessing.Pool(processes=num_workers) as pool:
                        pool.map(worker_task, work_items)

                    logger.info("All workers finished.")

        except Exception as e:
            logger.critical(f"Main process terminated unexpectedly: {e}", exc_info=True)
            import traceback

            traceback.print_exc()
        finally:
            # 7. Cleanup Logging
            logger.info("Shutting down logging listener...")
            log_queue.put(None)

            listener.join()

            # Explicitly delete the proxy and force GC to avoid resource_tracker warnings
            del log_queue
            gc.collect()

    finally:
        # 8. Shutdown Manager EXPLICITLY after listener is dead
        manager.shutdown()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
