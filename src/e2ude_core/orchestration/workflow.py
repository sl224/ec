import logging
import tempfile
from pathlib import Path
import sqlalchemy as sa

from e2ude_core.orchestration.scopes import session_scope, job_scope
from e2ude_core.orchestration.spec import JobSpec
from e2ude_core.services.zip_io import extract_specific_files
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.context import EtlContext

# Import specific functions
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


def process_zip(
    eng: sa.Engine,
    folder_id: int,
    zip_path: Path,
    context: EtlContext,
    extract_to_dir: Path = None,
):
    try:
        with session_scope(eng, folder_id, context) as session_manager:
            # --- Step 1: Evaluate State ---
            work_delta = get_folder_work_delta(
                eng, folder_id, scan_version=SCANNER_VERSION
            )

            # --- Step 2: Handle "Needs Scan" ---
            if work_delta.status == FolderState.NEEDS_SCAN:
                logger.info(f"Scan Required: {work_delta.scan_reason}")

                # Scan Job
                scan_spec = JobSpec(
                    pipeline_id=SCANNER_PIPELINE_ID,
                    job_name=f"MetadataScan: Folder {folder_id}",
                    target_name=FileMetadata.__tablename__,
                    handler_version=SCANNER_VERSION,
                    file_type="METADATA_SCAN",
                )

                row_count = 0
                with job_scope(session_manager, scan_spec) as job:
                    if job.active:
                        # Call the functional scanner
                        run_metadata_scan(
                            eng=eng,
                            folder_id=folder_id,
                            zip_path=zip_path,
                            job_updater=job.manager,
                        )
                        if job.manager._rows_uploaded_in_scope:
                            row_count = job.manager._rows_uploaded_in_scope

                # Audit Job
                registry_spec = JobSpec(
                    pipeline_id=SCANNER_PIPELINE_ID,
                    job_name=f"MetadataScan (Registry): Folder {folder_id}",
                    target_name=FileHashRegistry.__tablename__,
                    handler_version=SCANNER_VERSION,
                    file_type="METADATA_SCAN",
                )

                with job_scope(session_manager, registry_spec) as job:
                    if job.active:
                        job.manager.mark_completed(
                            "Completed via Primary Scan", rows=row_count
                        )

                work_delta = get_folder_work_delta(
                    eng, folder_id, scan_version=SCANNER_VERSION
                )

                if work_delta.status == FolderState.NEEDS_SCAN:
                    logger.error(
                        "State is still NEEDS_SCAN after successful execution. Aborting."
                    )
                    return

            # --- Step 3: Handle Processing ---
            if work_delta.status == FolderState.UP_TO_DATE:
                logger.info("Folder is UP_TO_DATE. No further action required.")
                return

            missing_items = work_delta.missing_items
            logger.info(
                f"Folder status is INCOMPLETE. Processing {len(missing_items)} pending datasets."
            )

            # Fetch file mappings (using helper function)
            db_files = fetch_existing_files_map(eng, folder_id)
            # db_files is a list of dict-like row mappings
            files_map = {f["hash_id"]: f for f in db_files}

            files_to_extract = set()
            for hash_id, _ in missing_items:
                file_info = files_map.get(hash_id)
                if file_info:
                    files_to_extract.add(file_info["relative_path"])

            if files_to_extract:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir_path = Path(temp_dir)

                    logger.info(f"Extracting {len(files_to_extract)} files...")
                    extract_specific_files(
                        zip_path, list(files_to_extract), temp_dir_path
                    )

                    for hash_id, dataset_key in missing_items:
                        file_info = files_map.get(hash_id)
                        if not file_info:
                            continue

                        full_path = temp_dir_path / file_info["relative_path"]
                        if not full_path.exists():
                            continue

                        # Direct Lookup into the Registry (no class instantiation)
                        handler_spec = HANDLER_REGISTRY.get(file_info["file_type"])
                        if not handler_spec:
                            continue

                        file_spec = JobSpec(
                            pipeline_id=handler_spec.pipeline_id,
                            job_name=f"{handler_spec.pipeline_id}: {file_info['relative_path']} [{dataset_key}]",
                            target_name=dataset_key,
                            handler_version=handler_spec.version,
                            file_id=file_info["id"],
                            hash_id=file_info["hash_id"],
                            file_type=file_info["file_type"],
                        )

                        with job_scope(session_manager, file_spec) as job:
                            if job.active:
                                # Call generic process function with specific configuration
                                process_file(
                                    eng=eng,
                                    spec=handler_spec,
                                    hash_id=hash_id,
                                    file_path=full_path,
                                    job_updater=job.manager,
                                    dataset_key=dataset_key,
                                )

    except Exception as e:
        logger.error(f"Critical failure in folder {folder_id}: {e}", exc_info=True)
