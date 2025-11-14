import logging
from tqdm import tqdm
from pathlib import Path
import sqlalchemy as sa
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict

# --- Core ETL Imports ---
from etude_core.orchestration.scopes import session_scope, job_scope
from etude_core.services.zip_io import UnzipContext
from etude_core.registry import HANDLER_REGISTRY
from etude_core.context import EtlContext
from etude_core.pipelines.scanner import MetadataScanHandler, FileToProcess
from etude_core.pipelines.base import DatasetKey

# --- NEW IMPORTS ---
from etude_core.pipelines.contexts import ScanJobContext, FileJobContext

# --- Database & Utils ---
from etude_core.db import access as sql_io
from etude_core.config import settings
from etude_core.db.models import (
    Base,
    ProcessingJob,
    ProcessingSession,
    StatusEnum,
    FileMetadata,
)


# (No changes to logging, FolderState, WorkDelta, or get_folder_work_delta)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FolderState(Enum):
    UP_TO_DATE = auto()
    PARTIAL = auto()


@dataclass
class WorkDelta:
    status: FolderState
    missing_items: List[Tuple[int, str]] = field(default_factory=list)


def get_folder_work_delta(eng: sa.Engine, folder_id: int) -> Optional[WorkDelta]:
    # (No changes to this function)
    with eng.connect() as conn:
        scan_complete = conn.execute(
            sa.select(ProcessingJob.id)
            .join(ProcessingSession, ProcessingJob.session_id == ProcessingSession.id)
            .where(
                ProcessingSession.folder_id == folder_id,
                ProcessingJob.pipeline_id == MetadataScanHandler.PIPELINE_ID,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
            .limit(1)
        ).scalar()

        if not scan_complete:
            return None

        actual_stmt = (
            sa.select(ProcessingJob.hash_id, ProcessingJob.dataset_key)
            .join(FileMetadata, ProcessingJob.file_id == FileMetadata.id)
            .where(
                FileMetadata.folder_id == folder_id,
                ProcessingJob.status == StatusEnum.COMPLETED,
            )
        )
        actual_work = set(conn.execute(actual_stmt).fetchall())

        expected_work = set()
        files = conn.execute(
            sa.select(FileMetadata.hash_id, FileMetadata.file_type).where(
                FileMetadata.folder_id == folder_id
            )
        ).fetchall()

        for hash_id, file_type in files:
            handler = HANDLER_REGISTRY.get(file_type)
            if handler:
                for key_enum in handler.TABLE_MAPPING.keys():
                    expected_work.add((hash_id, key_enum.name))

        missing_items = list(expected_work - actual_work)

        if not missing_items:
            return WorkDelta(status=FolderState.UP_TO_DATE)

        return WorkDelta(status=FolderState.PARTIAL, missing_items=missing_items)


def process_zip(
    eng: sa.Engine,
    folder_id: int,
    zip_path: Path,
    context: EtlContext,
    extract_to_dir: Path = None,
):
    # (No changes to outer try/session_scope/delta check)
    logger.info(f"--- Starting processing for FolderID {folder_id} ---")

    try:
        with session_scope(
            eng, folder_id, context.git_hash, context.user_name
        ) as session_manager:
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

            with UnzipContext(zip_path) as extract_dir_path:
                logger.info(
                    f"[Session: {session_manager.session_id}] Files extracted to {extract_dir_path}"
                )

                # 3. Metadata Scan (Runs as its own Job)
                files_to_process: List[FileToProcess] = []
                try:
                    scanner = MetadataScanHandler(
                        eng, folder_id, extract_dir_path.temp_dir
                    )

                    # --- FIX: Create ScanJobContext ---
                    scan_context = ScanJobContext(scanner)

                    # Pass the context object directly
                    with job_scope(session_manager, context=scan_context) as (
                        job_updater,
                        should_skip,
                    ):
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
                # (No changes to the setup of this loop)
                file_map: Dict[int, FileToProcess] = {
                    f.hash_id: f for f in files_to_process
                }
                type_to_key_map: Dict[str, Dict[str, DatasetKey]] = {}
                for file_type, handler in HANDLER_REGISTRY.items():
                    type_to_key_map[file_type] = {
                        member.name: member for member in handler.DATASET_ENUM
                    }

                work_items_to_process: List[Tuple[int, str]] = []

                if missing_items_lookup is None:
                    logger.info("New folder: processing all datasets for all files.")
                    for file in files_to_process:
                        handler = HANDLER_REGISTRY.get(file.file_type)
                        if handler:
                            for key_enum in handler.TABLE_MAPPING.keys():
                                work_items_to_process.append(
                                    (file.hash_id, key_enum.name)
                                )
                else:
                    work_items_to_process = missing_items_lookup
                    logger.info(
                        f"Partial folder: processing {len(work_items_to_process)} missing datasets."
                    )

                logger.info(
                    f"[Session: {session_manager.session_id}] Dispatching {len(work_items_to_process)} dataset jobs."
                )

                for hash_id, key_str in tqdm(
                    work_items_to_process, desc=f"Folder {folder_id} Jobs"
                ):
                    file = file_map.get(hash_id)
                    if not file:
                        logger.warning(
                            f"Skipping job for missing hash_id {hash_id} ({key_str})"
                        )
                        continue

                    handler = HANDLER_REGISTRY.get(file.file_type)
                    if not handler:
                        logger.warning(
                            f"Skipping job for missing handler {file.file_type} ({key_str})"
                        )
                        continue

                    key_map = type_to_key_map.get(file.file_type, {})
                    key_enum = key_map.get(key_str)
                    if not key_enum:
                        logger.warning(
                            f"Skipping job for unknown key '{key_str}' in {handler.PIPELINE_ID}"
                        )
                        continue

                    # B. Execute Job (one per dataset)
                    try:
                        # --- FIX: Create FileJobContext ---
                        file_context = FileJobContext(handler, file, key_enum)

                        # Pass the context object directly
                        with job_scope(session_manager, context=file_context) as (
                            job_updater,
                            should_skip,
                        ):
                            if should_skip:
                                continue

                            handler.run(
                                eng=eng,
                                hash_id=file.hash_id,
                                file_path=file.full_path,
                                job_updater=job_updater,
                                keys_to_process=[key_enum],
                            )

                    except Exception as e:
                        logger.error(
                            f"Handler {handler.PIPELINE_ID} failed for FileID {file.file_id} / Key {key_str}: {e}"
                        )
                        pass  # job_scope will mark as ERROR

    except Exception as e:
        logger.error(
            f"ETL process failed critically for FolderID {folder_id}: {e}",
            exc_info=True,
        )
    finally:
        logger.info(f"--- Finished processing for FolderID {folder_id} ---")


# --- Entry Point ---

if __name__ == "__main__":
    # (No changes to entry point)
    logger.info(f"Connecting to database type: {settings.database.type}")
    eng = sql_io.get_engine(settings.database)

    logger.info("Ensuring database schema exists...")
    Base.metadata.drop_all(eng)
    Base.metadata.create_all(eng)

    ctx = EtlContext.capture()
    logger.info(f"Execution Context: {ctx}")

    STATIC_ASSETS_ROOT = Path("tests/static_assets")
    test_zip = (
        STATIC_ASSETS_ROOT / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
    )

    id_paths = [(-i, test_zip) for i in range(10)][-1:]

    logger.info(f"Found {len(id_paths)} folders to process.")

    for folder_id, zip_path_str in tqdm(id_paths, desc="Overall Progress"):
        zip_path = Path(zip_path_str)

        if not zip_path.exists():
            logger.warning(f"Zip path does not exist, skipping: {zip_path}")
            continue

        process_zip(
            eng=eng,
            folder_id=folder_id,
            zip_path=zip_path,
            context=ctx,
        )
