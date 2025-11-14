import logging
from tqdm import tqdm
from pathlib import Path

# --- Core ETL Imports ---
from etude_core.context import EtlContext

# Renamed import:
from etude_core.orchestration.workflow import process_zip

from etude_core.db import access as sql_io
from etude_core.config import settings
from etude_core.db.setup import initialize_database, get_or_create_folder

# --- Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# --- Entry Point ---

if __name__ == "__main__":
    logger.info(f"Connecting to database type: {settings.database.type}")
    eng = sql_io.get_engine(settings.database)

    # 1. Initialize DB
    # We set reset_tables=True because this is a dev/test script.
    # For production, this would be `reset_tables=False`.
    initialize_database(eng, reset_tables=True)

    # 2. Capture Context
    ctx = EtlContext.capture()
    logger.info(f"Execution Context: {ctx}")

    # 3. Setup Test Data
    STATIC_ASSETS_ROOT = Path("tests/static_assets")
    test_zip = (
        STATIC_ASSETS_ROOT / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
    )

    # Use a list of (folder_id, path_to_zip) tuples for deterministic ordering
    id_paths = [(i, p) for i, p in enumerate(STATIC_ASSETS_ROOT.glob("**/zips/*.zip"))]
    logger.info(f"Found {len(id_paths)} folders to process.")

    # 4. Main Processing Loop
    for folder_id, zip_path_str in tqdm(id_paths, desc="Overall Progress"):
        zip_path = Path(zip_path_str)

        if not zip_path.exists():
            logger.warning(f"Zip path does not exist, skipping: {zip_path}")
            continue

        # Ensure the parent folder exists before processing
        if not get_or_create_folder(eng, folder_id, str(zip_path)):
            logger.warning(f"Skipping folder {folder_id} due to parent creation error.")
            continue

        # Call the main orchestration logic
        process_zip(
            eng=eng,
            folder_id=folder_id,
            zip_path=zip_path,
            context=ctx,
        )
