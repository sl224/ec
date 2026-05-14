import logging
import sys
import time
from pathlib import Path

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import (
    initialize_database,
    load_directory_scan_cache,
    record_directory_snapshots,
    register_archives_bulk,
)
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.pipeline import StagingPipeline
from e2ude_core.orchestration.runs import cull_stale_runs
from e2ude_core.orchestration.state import load_archives_requiring_work
from e2ude_core.services.discovery import DiscoveryMode, discover_archives

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

        discovery_started_at = time.perf_counter()
        known_directory_states = load_directory_scan_cache(
            main_eng, root_path=scan_root
        )
        discovery_result = discover_archives(
            scan_root,
            known_directory_states=known_directory_states,
            mode=DiscoveryMode(settings.runtime.discovery_mode),
            max_workers=settings.runtime.discovery_workers,
        )
        record_directory_snapshots(
            main_eng,
            discovery_result.directory_snapshots,
            missing_paths=discovery_result.missing_directory_paths,
        )

        register_archives_bulk(
            main_eng,
            list(discovery_result.archives),
            scanned_directory_paths=discovery_result.scanned_directory_paths,
            missing_directory_paths=discovery_result.missing_directory_paths,
        )
        logger.info(
            "Discovery summary: duration=%.2fs archives=%s scanned_dirs=%s "
            "archive_dir_scans=%s frontier_dir_scans=%s skipped_dirs=%s "
            "missing_dirs=%s",
            time.perf_counter() - discovery_started_at,
            len(discovery_result.archives),
            discovery_result.scanned_directory_count,
            discovery_result.archive_directory_scan_count,
            discovery_result.frontier_directory_scan_count,
            discovery_result.skipped_directory_count,
            len(discovery_result.missing_directory_paths),
        )

        workable_map = load_archives_requiring_work(main_eng)

        if not workable_map:
            logger.info("All archives are up to date. Exiting.")
            return

        pipeline = StagingPipeline(
            db_settings=settings.database,
            archive_id_map=workable_map,
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
        raise SystemExit(1) from e
    finally:
        main_eng.dispose()
        logger.info("Exiting.")


if __name__ == "__main__":
    main()
