import logging
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import List

import sqlalchemy as sa

from e2ude_core.orchestration.scopes import session_scope, job_scope
from e2ude_core.orchestration.spec import JobSpec
from e2ude_core.services.zip_io import extract_specific_files
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
    try:
        with session_scope(eng, folder_id, context) as session_manager:
            # --- Step 1: State Check ---
            work_delta = get_folder_work_delta(
                eng, folder_id, scan_version=SCANNER_VERSION
            )

            # --- Step 2: Scan (if needed) ---
            if work_delta.status == FolderState.NEEDS_SCAN:
                # ... (Scan logic remains the same) ...
                scan_spec = JobSpec(
                    pipeline_id=SCANNER_PIPELINE_ID,
                    job_name=f"MetadataScan: Folder {folder_id}",
                    target_name=FileMetadata.__tablename__,
                    handler_version=SCANNER_VERSION,
                    file_type="METADATA_SCAN",
                )
                with job_scope(session_manager, scan_spec) as job:
                    if job.active:
                        run_metadata_scan(eng, folder_id, staged_path, job.manager)

                # Re-evaluate delta after scan
                work_delta = get_folder_work_delta(
                    eng, folder_id, scan_version=SCANNER_VERSION
                )

            if work_delta.status == FolderState.UP_TO_DATE:
                return

            # --- Step 3: Grouping & Execution ---
            
            # Fetch file info to map Hash -> Path/Type
            db_files = fetch_existing_files_map(eng, folder_id)
            files_map = {f["hash_id"]: f for f in db_files}

            # Group missing tables by HashID
            # Map: hash_id -> List[table_name]
            work_batches = defaultdict(list)
            for hash_id, table_name in work_delta.missing_items:
                work_batches[hash_id].append(table_name)

            logger.info(f"Processing {len(work_batches)} files with missing data.")

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

                # Create ONE job for the file (processing N tables)
                # target_name is informational here, we can join the table names or say "BATCH"
                target_summary = "BATCH" if len(missing_tables) > 1 else missing_tables[0]
                
                file_spec = JobSpec(
                    pipeline_id=handler_spec.pipeline_id,
                    job_name=f"{handler_spec.pipeline_id}: {file_info['relative_path']}",
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
                            target_tables=missing_tables, # Pass the list!
                            db_workers=db_workers
                        )

    except Exception as e:
        logger.error(f"Critical failure in folder {folder_id}: {e}", exc_info=True)