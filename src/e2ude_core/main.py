import logging
from tqdm import tqdm
from pathlib import Path

# --- Core ETL Imports ---
from e2ude_core.context import EtlContext

# Renamed import:
from e2ude_core.orchestration.workflow import process_zip

from e2ude_core.db import access as sql_io
from e2ude_core.config import settings
from e2ude_core.db.setup import initialize_database, get_or_create_folder
from sqlalchemy import text

# --- Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# --- Entry Point ---
def get_data(eng):
    # 3. Setup Test Data: Pull from the Database Table
    query = """
    SELECT [FolderID], [FolderPath]
    FROM [AnalyticsDataMart].[E2D_METADATA].[FOLDER]
    ORDER BY [FolderDatetime] DESC
    """

    # We only need FolderID and FolderPath for the processing loop
    with eng.connect() as conn:
        id_paths = conn.execute(text(query)).fetchall()

    logger.info(f"Found {len(id_paths)} folders to process via DB query.")
    return id_paths


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
    id_paths = get_data(eng)

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
