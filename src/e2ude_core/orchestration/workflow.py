import logging
import tempfile
import shutil
from pathlib import Path
from tqdm import tqdm
import sqlalchemy as sa
from typing import List, Tuple, Optional, Dict

# --- Core ETL Imports ---
from e2ude_core.orchestration.scopes import session_scope, job_scope
from e2ude_core.services.zip_io import RecursiveZipScanner, recursive_unzip
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.context import EtlContext
from e2ude_core.pipelines.scanner import MetadataScanHandler, FileToProcess
from e2ude_core.pipelines.contexts import ScanJobContext, FileJobContext
from e2ude_core.orchestration.state import get_folder_work_delta, FolderState

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
    Uses Lazy Extraction: Scans first, only unzips if work is needed.
    """
    try:
        with session_scope(eng, folder_id, context) as session_manager:
            
            # =========================================
            # PHASE 1: IN-MEMORY SCAN & CATALOG
            # =========================================
            logger.info("Starting Metadata Scan (In-Memory)...")
            
            # 1. Scan the zip structure in-memory to get file list & hashes
            scanner = RecursiveZipScanner(zip_path)
            raw_files = scanner.scan()
            
            if not raw_files:
                logger.warning("Zip file appears empty or unreadable.")
                return

            # 2. Initialize Handler with a dummy path (since we haven't extracted yet)
            # The handler's `upsert` method doesn't need the physical path, just the metadata dicts.
            meta_handler = MetadataScanHandler(eng, folder_id, Path("."))
            
            # 3. Upsert metadata to DB (Public API)
            # This updates `metadata_file` and `metadata_hash_registry`
            meta_handler.upsert(raw_files)
            
            # =========================================
            # PHASE 2: STATE CALCULATION
            # =========================================
            work_delta = get_folder_work_delta(eng, folder_id)
            
            if work_delta is None:
                 # Should not happen if upsert worked, unless DB commit failed
                 logger.error("Work delta is None after scan. Aborting.")
                 return

            if work_delta.status == FolderState.UP_TO_DATE:
                logger.info("Folder is 100% complete. Skipping physical extraction.")
                return
            
            missing_items = work_delta.missing_items
            logger.info(f"Folder partially complete. Processing {len(missing_items)} missing datasets.")

            # =========================================
            # PHASE 3: PHYSICAL EXTRACTION
            # =========================================
            # Only now do we touch the disk
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                logger.info(f"Work detected. Extracting to {temp_dir_path}...")
                
                # Extract everything (handling nested zips)
                recursive_unzip(temp_dir_path, zip_path)
                
                # Re-fetch file list from DB to get the correct FileIDs and Relative Paths
                # Update their full_path to point to our temp dir
                db_files = meta_handler.fetch_existing_files()
                
                # Create lookup map
                files_map = {f.hash_id: f for f in db_files}

                # =========================================
                # PHASE 4: EXECUTION LOOP
                # =========================================
                logger.info(f"Dispatching {len(missing_items)} dataset jobs.")
                
                for hash_id, dataset_key in tqdm(missing_items, desc=f"Folder {folder_id} Jobs"):
                    
                    file_info = files_map.get(hash_id)
                    if not file_info:
                        logger.warning(f"Missing file info for hash {hash_id}. Skipping.")
                        continue
                    
                    # Update path to be absolute based on current temp_dir
                    full_path = temp_dir_path / file_info.relative_path
                    
                    handler = HANDLER_REGISTRY.get(file_info.file_type)
                    if not handler:
                        # Should have been filtered by expected_work logic, but safe check
                        continue

                    file_context = FileJobContext(handler, file_info, dataset_key)
                    
                    # Use new simplified scope
                    with job_scope(session_manager, file_context) as job:
                        if job.active:
                             handler.run(
                                eng=eng, 
                                hash_id=hash_id, 
                                file_path=full_path, 
                                job_updater=job.manager, 
                                keys_to_process=[dataset_key]
                            )

    except Exception as e:
        logger.error(
            f"ETL process failed critically for FolderID {folder_id}: {e}",
            exc_info=True,
        )
