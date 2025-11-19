import logging
import tempfile
from pathlib import Path
from tqdm import tqdm
import sqlalchemy as sa

from e2ude_core.orchestration.scopes import session_scope, job_scope
from e2ude_core.orchestration.spec import JobSpec
from e2ude_core.services.zip_io import extract_specific_files
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.context import EtlContext
from e2ude_core.pipelines.scanner import MetadataScanHandler
from e2ude_core.orchestration.state import get_folder_work_delta, FolderState
from e2ude_core.db.models import FileMetadata

logger = logging.getLogger(__name__)

def process_zip(
    eng: sa.Engine,
    folder_id: int,
    zip_path: Path,
    context: EtlContext,
    extract_to_dir: Path = None,
):
    """
    Orchestrates the ETL workflow.
    Optimized: Checks DB state BEFORE initializing session or touching zip.
    """
    
    # 0. PRE-CHECK (Optimization)
    try:
        pre_check_delta = get_folder_work_delta(eng, folder_id, scan_version=MetadataScanHandler.VERSION)
        if pre_check_delta and pre_check_delta.status == FolderState.UP_TO_DATE:
            # logger.info(f"Folder {folder_id} is already UP_TO_DATE. Skipping.")
            return
    except Exception as e:
        logger.warning(f"Pre-check failed for folder {folder_id} (proceeding to full logic): {e}")

    try:
        with session_scope(eng, folder_id, context) as session_manager:
            
            # =========================================
            # PHASE 1: SCAN JOB
            # =========================================
            logger.info("Starting Metadata Scan...")
            
            scan_spec = JobSpec(
                pipeline_id=MetadataScanHandler.PIPELINE_ID,
                job_name=f"MetadataScan: Folder {folder_id}",
                target_name=FileMetadata.__tablename__, # Explicit Output Table
                handler_version=MetadataScanHandler.VERSION,
                file_type="N/A"
            )

            with job_scope(session_manager, scan_spec) as job:
                if job.active:
                    # Delegate execution to the handler, just like standard files
                    meta_handler = MetadataScanHandler(eng, folder_id, Path("."))
                    meta_handler.run(
                        eng=eng,
                        hash_id=None, 
                        file_path=zip_path, 
                        job_updater=job.manager,
                        keys_to_process=None
                    )
                    meta_handler.run(zip_path, job.manager)
            
            # =========================================
            # PHASE 2: STATE CALCULATION
            # =========================================
            work_delta = get_folder_work_delta(eng, folder_id, scan_version=MetadataScanHandler.VERSION)
            
            if work_delta is None:
                 logger.error("Work delta is None after scan. Aborting.")
                 return

            if work_delta.status == FolderState.UP_TO_DATE:
                logger.info("Folder is 100% complete. Skipping physical extraction.")
                return
            
            missing_items = work_delta.missing_items
            logger.info(f"Folder partially complete. Processing {len(missing_items)} missing datasets.")

            # =========================================
            # PHASE 3: SELECTIVE EXTRACTION & PROCESSING
            # =========================================
            # Re-init handler to fetch DB state
            meta_handler = MetadataScanHandler(eng, folder_id, Path("."))
            db_files = meta_handler.fetch_existing_files()
            files_map = {f.hash_id: f for f in db_files}

            files_to_extract = set()
            for hash_id, _ in missing_items:
                file_info = files_map.get(hash_id)
                if file_info:
                    files_to_extract.add(file_info.relative_path)
            
            if files_to_extract:
                with tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir_path = Path(temp_dir)
                    
                    logger.info(f"Extracting {len(files_to_extract)} files to {temp_dir_path}...")
                    extract_specific_files(zip_path, list(files_to_extract), temp_dir_path)

                    for hash_id, dataset_key in tqdm(missing_items, desc=f"Folder {folder_id} Jobs"):
                        
                        file_info = files_map.get(hash_id)
                        if not file_info: continue
                        
                        full_path = temp_dir_path / file_info.relative_path
                        if not full_path.exists(): continue

                        handler = HANDLER_REGISTRY.get(file_info.file_type)
                        if not handler: continue

                        file_spec = JobSpec(
                            pipeline_id=handler.PIPELINE_ID,
                            job_name=f"{handler.PIPELINE_ID}: {file_info.relative_path} [{dataset_key}]",
                            target_name=dataset_key,
                            handler_version=handler.VERSION,
                            file_type=file_info.file_type,
                            file_id=file_info.file_id,
                            hash_id=file_info.hash_id
                        )
                        
                        with job_scope(session_manager, file_spec) as job:
                            if job.active:
                                handler.run(
                                    eng=eng, 
                                    hash_id=hash_id, 
                                    file_path=full_path, 
                                    job_updater=job.manager, 
                                    keys_to_process=[dataset_key]
                                )

    except Exception as e:
        logger.error(f"Critical failure in folder {folder_id}: {e}", exc_info=True)