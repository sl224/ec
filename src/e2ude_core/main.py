import logging
import sys
import time
from pathlib import Path

from e2ude_core.config import settings
from e2ude_core.db import access as sql_io
from e2ude_core.db.setup import (
    initialize_database,
    register_archives_bulk,
)
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.pipeline import ArchivePipeline
from e2ude_core.orchestration.runs import cull_stale_runs
from e2ude_core.orchestration.state import load_archives_requiring_work
from e2ude_core.services.discovery import discover_archives

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
    logger.info("Refresh starting. staging_root=%s", staging_root)

    main_eng = sql_io.get_engine(settings.database, default_pool_size=64)

    try:
        scan_root = _resolve_scan_root()
        if not scan_root.exists():
            raise FileNotFoundError(f"Scan root not found: {scan_root}")

        initialize_database(main_eng, reset_tables=False)
        culled = cull_stale_runs(main_eng)
        if culled["jobs"] or culled["sessions"]:
            logger.warning(
                "Culled %s stale jobs across %s stale sessions before discovery.",
                culled["jobs"],
                culled["sessions"],
            )

        logger.info("Discovery: scanning directories.")
        discovery_started_at = time.perf_counter()
        discovery_result = discover_archives(
            scan_root,
            max_workers=settings.runtime.discovery_workers,
        )
        logger.info(
            "Discovery summary: duration=%.2fs archives=%s scanned_dirs=%s",
            time.perf_counter() - discovery_started_at,
            len(discovery_result.archives),
            discovery_result.scanned_directory_count,
        )

        logger.info("Register: writing archive locators.")
        register_started_at = time.perf_counter()
        register_archives_bulk(
            main_eng,
            list(discovery_result.archives),
        )
        logger.info(
            "Register summary: duration=%.2fs archives=%s scanned_dirs=%s",
            time.perf_counter() - register_started_at,
            len(discovery_result.archives),
            discovery_result.scanned_directory_count,
        )

        logger.info("Plan: selecting archive work.")
        workable_map = load_archives_requiring_work(main_eng)

        if not workable_map:
            logger.info("All archives are up to date. Exiting.")
            return

        logger.info(
            "Pipeline: processing %s archives.",
            len(workable_map),
        )
        pipeline = ArchivePipeline(
            db_settings=settings.database,
            archive_id_map=workable_map,
            staging_root=staging_root,
            process_workers=settings.runtime.process_workers,
        )
        failed_count = pipeline.run()
        if failed_count:
            raise SystemExit(1)

    except KeyboardInterrupt:
        print("\nInterrupted by Ctrl+C.")
        logger.warning("Refresh interrupted.")

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
        logger.critical("Refresh failed: %s", e, exc_info=True)
        raise SystemExit(1) from e
    finally:
        main_eng.dispose()
        logger.info("Refresh finished.")


if __name__ == "__main__":
    main()
