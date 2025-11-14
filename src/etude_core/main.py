import logging
from tqdm import tqdm
from pathlib import Path
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

# --- Core ETL Imports ---
from etude_core.orchestration.scopes import session_scope, job_scope
from etude_core.services.zip_io import UnzipContext
from etude_core.registry import HANDLER_REGISTRY
from etude_core.context import EtlContext
from etude_core.pipelines.scanner import MetadataScanHandler

# --- Database & Utils ---
from etude_core.db import access as sql_io
from etude_core.config import settings
from etude_core.db.models import Base, ProcessingJob,ProcessingSession, StatusEnum, FileMetadata




# --- Configuration ---
# Configure logging to print to stdout
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class FolderState(Enum):
    UP_TO_DATE = auto()   # Folder in DB, all jobs done
    PARTIAL = auto()      # Folder in DB, work remaining

@dataclass
class WorkDelta:
    status: FolderState
    # List of (hash_id, dataset_key) that need processing
    missing_items: List[Tuple[int, str]] = field(default_factory=list)
def get_folder_work_delta(eng: sa.Engine, folder_id: int) -> Optional[WorkDelta]:
    """
    Determines the processing state of a folder.
    - Returns None if the folder has never been successfully scanned.
    - Returns WorkDelta if the folder has been scanned.
    """
    
    with eng.connect() as conn:
        
        # 1. CHECK SCAN STATUS
        # Has the MetadataScanHandler *ever* completed for this folder?
        scan_complete = conn.execute(
            sa.select(ProcessingJob.id)
            .join(ProcessingSession, ProcessingJob.session_id == ProcessingSession.id)
            .where(
                ProcessingSession.folder_id == folder_id,
                ProcessingJob.pipeline_id == MetadataScanHandler.PIPELINE_ID,
                ProcessingJob.status == StatusEnum.COMPLETED
            )
            .limit(1)
        ).scalar()

        if not scan_complete:
            # If no scan has ever completed, we MUST treat it as NEW.
            # Return None to signal the caller to perform the scan.
            return None

        # --- If we are here, a scan IS complete. We can trust FileMetadata ---

        # 2. GET ACTUAL STATE (What data jobs are done?)
        actual_stmt = (
            sa.select(ProcessingJob.hash_id, ProcessingJob.dataset_key)
            .join(FileMetadata, ProcessingJob.file_id == FileMetadata.id)
            .where(
                FileMetadata.folder_id == folder_id,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
        )
        actual_work = set(conn.execute(actual_stmt).fetchall())

        # 3. GET EXPECTED STATE (What files did the scan find?)
        expected_work = set()

        files = conn.execute(
            sa.select(FileMetadata.hash_id, FileMetadata.file_type).where(
                FileMetadata.folder_id == folder_id
            )
        ).fetchall()

        # If `files` is empty, this loop is skipped.
        for hash_id, file_type in files:
            handler = HANDLER_REGISTRY.get(file_type)
            if handler:
                for key in handler.declared_datasets:
                    expected_work.add((hash_id, key))

        # 4. CALCULATE DELTA
        missing_items = list(expected_work - actual_work)

        if not missing_items:
            # If `files` was empty, `expected_work` is {}, `actual_work` is {}.
            # `missing_items` is []. This correctly returns UP_TO_DATE.
            return WorkDelta(status=FolderState.UP_TO_DATE)

        return WorkDelta(status=FolderState.PARTIAL, missing_items=missing_items)

def process_zip(
    eng: sa.Engine,
    folder_id: int,
    zip_path: Path,
    context: EtlContext,
    extract_to_dir: Path = None,
):
    """
    Orchestrates the ETL process for a single zipped folder.

    Steps:
    1. Starts a Processing Session (Audit Log).
    2. Unzips the archive (Temp or Persistent).
    3. Scans/Catalogs all files (MetadataScan).
    4. Dispatches files to specific Handlers based on Registry.
    """
    logger.info(f"--- Starting processing for FolderID {folder_id} ---")

    try:
        # 1. Start Session
        # We pass context details to the session manager for auditing (User + Git Hash)
        with session_scope(
            eng, folder_id, context.git_hash, context.user_name
        ) as session_manager:
            # A. Check Work Delta
            work_delta = get_folder_work_delta(eng, folder_id)
            missing_items_lookup = None

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
            # UnzipContext handles creation and auto-cleanup of the temp directory
            with UnzipContext(zip_path) as extract_dir_path:
                logger.info(
                    f"[Session: {session_manager.session_id}] Files extracted to {extract_dir_path}"
                )

                # 3. Metadata Scan (Runs as its own Job)
                # This catalogs all files and calculates their HashIDs
                files_to_process = []
                try:
                    scanner = MetadataScanHandler(
                        eng, folder_id, extract_dir_path.temp_dir
                    )

                    # Use job_scope for the folder-level scan (file_to_process=None)
                    with job_scope(
                        session_manager, handler_instance=scanner, file_to_process=None
                    ) as (job_updater, should_skip):
                        # Even if skipped, this returns the file list from DB so we can process children
                        files_to_process = scanner.run(job_updater, should_skip)

                except Exception as e:
                    logger.error(
                        f"CRITICAL: MetadataScanHandler failed: {e}. Aborting session.",
                        exc_info=True,
                    )
                    raise  # Fails the session

                if not files_to_process:
                    logger.warning(
                        f"[Session: {session_manager.session_id}] No files found to process."
                    )
                    return

                # 4. File Dispatch Loop
                logger.info(
                    f"[Session: {session_manager.session_id}] Dispatching {len(files_to_process)} file jobs."
                )

                for file in tqdm(files_to_process, desc=f"Folder {folder_id}"):
                    # A. Lookup Handler in Registry
                    handler = HANDLER_REGISTRY.get(file.file_type)
                    if not handler:
                        logger.debug(f"No handler for type {file.file_type}")
                        continue

                    # OPTIMIZATION:
                    # Only run the specific keys that were identified as missing
                    keys_needed = []
                    if missing_items_lookup:
                        keys_needed = [k for (h, k) in missing_items_lookup if h == file.hash_id]

                    if missing_items_lookup is not None and not keys_needed:
                        logger.info("No keys needed. Skipping...")
                        continue  # Skip file

                    # B. Execute Job
                    try:
                        # job_scope handles Skip Logic (Idempotency) checking the HashID
                        with job_scope(
                            session_manager,
                            handler_instance=handler,
                            file_to_process=file,
                        ) as (job_updater, should_skip):

                            if should_skip:
                                logger.info("Skipping...")
                                continue

                            handler.run(
                                eng=eng,
                                hash_id=file.hash_id,
                                file_path=file.full_path,
                                job_updater=job_updater,
                            )

                    except Exception as e:
                        # Catch handler failures so one bad file doesn't kill the whole folder
                        # We log the FileID here for debugging context
                        logger.error(
                            f"Handler {handler.PIPELINE_ID} failed for FileID {file.file_id}: {e}"
                        )
                        pass

    except Exception as e:
        # Catch critical errors (Unzip failure, DB connection loss)
        logger.error(
            f"ETL process failed critically for FolderID {folder_id}: {e}",
            exc_info=True,
        )
    finally:
        logger.info(f"--- Finished processing for FolderID {folder_id} ---")


# --- Entry Point ---

if __name__ == "__main__":
    # 1. Setup Database Connection
    # Adjust connection strings as needed for your environment
    logger.info(f"Connecting to database type: {settings.database.type}")
    eng = sql_io.get_engine(settings.database)

    # 2. Ensure Tables Exist
    # Creates tables for Job Status, File Metadata, and Data Models if missing
    logger.info("Ensuring database schema exists...")
    Base.metadata.drop_all(eng)
    Base.metadata.create_all(eng)

    # 3. Capture Context (Once at Startup)
    # Captures Git Hash, User Name, and Hostname for auditing
    ctx = EtlContext.capture()
    logger.info(f"Execution Context: {ctx}")

    STATIC_ASSETS_ROOT = Path("tests/static_assets")
    test_zip = (
        STATIC_ASSETS_ROOT / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
    )

    id_paths = [(-i, test_zip) for i in range(10)][-1: ]

    logger.info(f"Found {len(id_paths)} folders to process.")

    # 5. Process Loop
    for folder_id, zip_path_str in tqdm(id_paths, desc="Overall Progress"):
        zip_path = Path(zip_path_str)

        if not zip_path.exists():
            logger.warning(f"Zip path does not exist, skipping: {zip_path}")
            continue

        # Call the pure orchestrator function
        process_zip(
            eng=eng,
            folder_id=folder_id,
            zip_path=zip_path,
            context=ctx,  # Pass the captured context
            # extract_to_dir=Path("C:/Temp/Debug") # Optional: Set specific dir for debugging
        )
