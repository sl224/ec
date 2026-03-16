import logging
import sys
import time
from pathlib import Path

from tqdm import tqdm

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.managers import cull_stale_runs
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.orchestration.state import select_folders_requiring_work
from e2ude_core.services.discovery import discover_network_zips
from e2ude_core.pipelines.scanner import SCANNER_VERSION

logger = logging.getLogger(__name__)


def _resolve_staging_root() -> Path:
    staging_root = settings.paths.staging_root
    staging_root.mkdir(parents=True, exist_ok=True)
    return staging_root


def _resolve_scan_root() -> Path:
    if settings.paths.scan_root is None:
        raise ValueError("Scan root is not configured.")
    return settings.paths.scan_root


def main():
    staging_root = _resolve_staging_root()
    setup_logging(settings)
    logger.info("Starting pipeline. staging=%s", staging_root)

    main_eng = sql_io.get_engine(settings.database, default_pool_size=64)

    try:
        initialize_database(main_eng, reset_tables=False)
        culled = cull_stale_runs(main_eng)
        if culled["jobs"] or culled["sessions"]:
            logger.warning(
                "Culled %s stale jobs across %s stale sessions before discovery.",
                culled["jobs"],
                culled["sessions"],
            )

        scan_root = _resolve_scan_root()
        if not scan_root.exists():
            logger.error(f"Scan root not found: {scan_root}")
            return

        valid_paths = discover_network_zips(
            scan_root,
            max_workers=settings.runtime.discovery_workers,
        )
        if not valid_paths:
            logger.info("No zips found.")
            return

        all_folders_map = register_folders_bulk(main_eng, valid_paths)

        if not all_folders_map:
            logger.info("No folders registered.")
            return

        with tqdm(
            total=len(all_folders_map),
            desc="Checking DB State",
            unit="folder",
        ) as pbar:
            workable_map = select_folders_requiring_work(
                main_eng,
                all_folders_map,
                SCANNER_VERSION,
                progress_callback=pbar.update,
            )

        if not workable_map:
            logger.info("All folders are up to date. Exiting.")
            return

        pipeline = StagingPipeline(
            db_settings=settings.database,
            folder_id_map=workable_map,
            staging_root=staging_root,
            buffer_size=settings.runtime.pipeline_buffer_size,
            unzip_workers=settings.runtime.unzip_workers,
            process_workers=settings.runtime.process_workers,
            db_write_workers=settings.runtime.db_write_workers,
        )
        pipeline.run()

    except KeyboardInterrupt:
        print("\n[!] Force Quit (Ctrl+C) Detected.")
        logger.warning("Killing all threads...")

        if settings.diagnostics.enable_viztracer:
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

        try:
            sys.exit(1)
        except SystemExit:
            import os

            os._exit(1)

    except Exception as e:
        logger.critical(f"Fatal Error: {e}", exc_info=True)
    finally:
        main_eng.dispose()
        logger.info("Exiting.")


if __name__ == "__main__":
    main()
