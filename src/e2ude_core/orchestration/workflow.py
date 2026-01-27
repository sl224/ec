import logging
from collections import defaultdict
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.orchestration.scopes import session_scope, job_scope
from e2ude_core.orchestration.spec import JobSpec

# Removed unused imports: tempfile, extract_specific_files
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.context import EtlContext

from e2ude_core.pipelines.scanner import (
    run_metadata_scan,
    fetch_existing_files_map,
    SCANNER_PIPELINE_ID,
    SCANNER_VERSION,
)
from e2ude_core.pipelines.base import process_file
from e2ude_core.orchestration.state import get_folder_work_delta, FolderState
from e2ude_core.db.models import FileMetadata, FileHashRegistry

logger = logging.getLogger(__name__)


def process_staged_directory(
    eng: sa.Engine,
    folder_id: int,
    staged_path: Path,
    context: EtlContext,
    db_workers: int = 4,
):
    """
    Orchestrates the processing of a fully staged (unzipped/exploded) folder.
    """
    try:
        with session_scope(eng, folder_id, context) as session_manager:
            # --- Step 1: State Check ---
            work_delta = get_folder_work_delta(
                eng, folder_id, scan_version=SCANNER_VERSION
            )

            # --- Step 2: Scan (if needed) ---
            if work_delta.status == FolderState.NEEDS_SCAN:
                logger.info(f"Scan Required: {work_delta.scan_reason}")

                scan_spec = JobSpec(
                    pipeline_id=SCANNER_PIPELINE_ID,
                    job_name=f"MetadataScan: Folder {folder_id}",
                    target_name=FileMetadata.__tablename__,
                    handler_version=SCANNER_VERSION,
                    file_type="METADATA_SCAN",
                )
                with job_scope(session_manager, scan_spec) as job:
                    if job.active:
                        # Pass the directory path; the new scanner handles the recursion
                        run_metadata_scan(eng, folder_id, staged_path, job.manager)

                # Audit Job for Registry (Log completion)
                registry_spec = JobSpec(
                    pipeline_id=SCANNER_PIPELINE_ID,
                    job_name=f"MetadataScan (Registry): Folder {folder_id}",
                    target_name=FileHashRegistry.__tablename__,
                    handler_version=SCANNER_VERSION,
                    file_type="METADATA_SCAN",
                )
                with job_scope(session_manager, registry_spec) as job:
                    if job.active:
                        job.manager.mark_completed("Completed via Primary Scan")

                # Re-evaluate delta after scan
                work_delta = get_folder_work_delta(
                    eng, folder_id, scan_version=SCANNER_VERSION
                )

            if work_delta.status == FolderState.UP_TO_DATE:
                logger.info("Folder is UP_TO_DATE. No further action required.")
                return

            # --- Step 3: Grouping & Execution ---

            # Fetch file info to map Hash -> Path/Type
            db_files = fetch_existing_files_map(eng, folder_id)
            files_map = {f["hash_id"]: f for f in db_files}

            # Group missing tables by HashID to avoid re-parsing the file for each table
            work_batches = defaultdict(list)
            for hash_id, table_name in work_delta.missing_items:
                work_batches[hash_id].append(table_name)

            logger.info(f"Processing {len(work_batches)} files with pending data.")

            for hash_id, missing_tables in work_batches.items():
                file_info = files_map.get(hash_id)
                if not file_info:
                    continue

                full_path = staged_path / file_info["relative_path"]
                if not full_path.exists():
                    logger.warning(f"File missing in staging: {full_path}")
                    continue

                handler_spec = HANDLER_REGISTRY.get(file_info["file_type"])
                if not handler_spec:
                    continue

                # Summarize target for the job log
                target_summary = (
                    "BATCH" if len(missing_tables) > 1 else missing_tables[0]
                )

                file_spec = JobSpec(
                    pipeline_id=handler_spec.pipeline_id,
                    job_name=f"{handler_spec.pipeline_id}: {file_info['relative_path']} [{target_summary}]",
                    target_name=target_summary,
                    handler_version=handler_spec.version,
                    file_id=file_info["id"],
                    hash_id=file_info["hash_id"],
                    file_type=file_info["file_type"],
                )

                with job_scope(session_manager, file_spec) as job:
                    if job.active:
                        process_file(
                            eng=eng,
                            spec=handler_spec,
                            hash_id=hash_id,
                            file_path=full_path,
                            job_updater=job.manager,
                            target_tables=missing_tables,  # Pass list of specific tables
                            db_workers=db_workers,  # Enable parallel DB writes
                        )

    except Exception as e:
        logger.error(f"Critical failure in folder {folder_id}: {e}", exc_info=True)
