import logging
from pathlib import Path
from tqdm import tqdm
import sqlalchemy as sa
from typing import List, Tuple, Optional, Dict

# --- Core ETL Imports ---
from etude_core.orchestration.scopes import session_scope, job_scope
from etude_core.services.zip_io import UnzipContext
from etude_core.registry import HANDLER_REGISTRY
from etude_core.context import EtlContext
from etude_core.pipelines.scanner import MetadataScanHandler, FileToProcess
from etude_core.pipelines.contexts import ScanJobContext, FileJobContext
from etude_core.orchestration.state import get_folder_work_delta, FolderState

logger = logging.getLogger(__name__)


def process_zip(
    eng: sa.Engine,
    folder_id: int,
    zip_path: Path,
    context: EtlContext,
    extract_to_dir: Path = None,
):
    """
    Orchestrates the ETL workflow for a single zipped folder.
    """
    try:
        with session_scope(
            eng, folder_id, context.git_hash, context.user_name
        ) as session_manager:
            # 1. Calculate State
            work_delta = get_folder_work_delta(eng, folder_id)
            missing_items_lookup: Optional[List[Tuple[int, str]]] = None

            if work_delta is None:
                logger.info("New folder detected. Proceeding to Unzip & Scan.")
            elif work_delta.status is FolderState.UP_TO_DATE:
                logger.info("Folder is 100% complete. Skipping.")
                return
            else:
                logger.info(
                    f"Folder partially complete. Processing {len(work_delta.missing_items)} missing datasets."
                )
                missing_items_lookup = work_delta.missing_items

            # 2. Unzip
            with UnzipContext(zip_path) as extract_dir_path:
                logger.info(
                    f"[Session: {session_manager.session_id}] Files extracted to {extract_dir_path}"
                )

                # 3. Metadata Scan
                files_to_process: List[FileToProcess] = []
                try:
                    scanner = MetadataScanHandler(
                        eng, folder_id, extract_dir_path.temp_dir
                    )
                    scan_context = ScanJobContext(scanner)
                    with job_scope(session_manager, context=scan_context) as (
                        job_updater,
                        should_skip,
                    ):
                        # scanner.run() -> list[FileToProcess]
                        files_to_process = scanner.run(job_updater, should_skip)
                except Exception as e:
                    logger.error(
                        f"CRITICAL: MetadataScanHandler failed: {e}. Aborting session.",
                        exc_info=True,
                    )
                    raise

                if not files_to_process:
                    logger.warning(
                        f"[Session: {session_manager.session_id}] No files found to process."
                    )
                    return

                # 4. File Dispatch Loop
                # Create a lookup map for efficient job dispatch
                file_map: Dict[int, FileToProcess] = {
                    f.hash_id: f for f in files_to_process
                }

                work_items_to_process: List[Tuple[int, str]] = []

                if missing_items_lookup is None:
                    # New folder: process all datasets for all files
                    logger.info("New folder: processing all datasets for all files.")
                    for file in files_to_process:
                        handler = HANDLER_REGISTRY.get(file.file_type)
                        if handler:
                            # Enumerate models from `handler.expected_models` to derive dataset keys (aligns with state.py).
                            for model in handler.expected_models:
                                work_items_to_process.append(
                                    (file.hash_id, model.__tablename__)
                                )
                else:
                    # Partial folder: only process the missing items
                    work_items_to_process = missing_items_lookup
                    logger.info(
                        f"Partial folder: processing {len(work_items_to_process)} missing datasets."
                    )

                logger.info(
                    f"[Session: {session_manager.session_id}] Dispatching {len(work_items_to_process)} dataset jobs."
                )

                # 5. Process work items
                # `dataset_key`: job-tracking key (replaces legacy 'table_name').
                for hash_id, dataset_key in tqdm(
                    work_items_to_process, desc=f"Folder {folder_id} Jobs"
                ):
                    file = file_map.get(hash_id)
                    if not file:
                        logger.warning(
                            f"Skipping job for missing hash_id {hash_id} ({dataset_key})"
                        )
                        continue

                    handler = HANDLER_REGISTRY.get(file.file_type)
                    if not handler:
                        logger.warning(
                            f"Skipping job for missing handler {file.file_type} ({dataset_key})"
                        )
                        continue

                    try:
                        # Context uses string `dataset_key` for job identification.
                        file_context = FileJobContext(handler, file, dataset_key)
                        with job_scope(session_manager, context=file_context) as (
                            job_updater,
                            should_skip,
                        ):
                            if should_skip:
                                continue

                            # Restrict handler execution to the specified `dataset_key`.
                            handler.run(
                                eng=eng,
                                hash_id=file.hash_id,
                                file_path=file.full_path,
                                job_updater=job_updater,
                                keys_to_process=[
                                    dataset_key
                                ],  # Pass the specific dataset_key
                            )

                    except Exception as e:
                        logger.error(
                            f"Handler {handler.PIPELINE_ID} failed for FileID {file.file_id} / Key {dataset_key}: {e}"
                        )
                        pass  # job_scope will mark as ERROR

    except Exception as e:
        logger.error(
            f"ETL process failed critically for FolderID {folder_id}: {e}",
            exc_info=True,
        )
