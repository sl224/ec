import logging
from pathlib import Path
from typing import Optional

import sqlalchemy as sa

from e2ude_core.orchestration.managers import JobManager
from e2ude_core.db import access as sql_io
from e2ude_core.registry import HandlerSpec
from e2ude_core.db.models import ArtifactManifest

logger = logging.getLogger(__name__)


def process_file(
    eng: sa.Engine,
    spec: HandlerSpec,
    hash_id: int,
    file_path: Path,
    job_updater: JobManager,
    dataset_key: Optional[str] = None,
):
    """
    Stateless function to execute the ETL logic for a single file.
    1. Parse
    2. Filter
    3. Atomic Replace (Delete/Insert)
    """
    pipeline_id = spec.pipeline_id
    logger.info(
        f"[{pipeline_id}] Processing HashID {hash_id} for key: {dataset_key or 'ALL'}"
    )

    # 1. Parse
    try:
        # Call the function pointer directly
        model_to_df_map = spec.parser_func(file_path)
    except Exception:
        logger.error(f"[{pipeline_id}] Parser failed for {file_path}", exc_info=True)
        raise

    # 2. Filter & Normalize
    # We only want the dataframe associated with the specific target table (dataset_key)
    # or all of them if dataset_key is None.
    payload = []

    # Pre-calculate valid table names for this handler
    valid_tables = {m.__tablename__: m for m in spec.expected_models}

    for model, df in model_to_df_map.items():
        table_name = model.__tablename__

        if table_name not in valid_tables:
            continue

        if dataset_key and table_name != dataset_key:
            continue

        payload.append((model, df))

    if not payload:
        logger.info(f"[{pipeline_id}] No data found for specified keys.")
        job_updater._rows_uploaded_in_scope = 0
        return

    # 3. Atomic Upload
    try:
        total_rows = 0
        row_count_sum = sum(len(item[1]) for item in payload)
        job_updater.mark_running(f"Uploading {row_count_sum} rows...")

        with eng.begin() as conn:
            for model, df in payload:
                if df.empty:
                    continue

                table_name = model.__tablename__

                # A. Prepare Data
                df_copy = df.copy()
                df_copy["hash_id"] = hash_id

                # B. Delete OLD Data (Atomic Replacement)
                if hasattr(model, "hash_id"):
                    conn.execute(
                        model.__table__.delete().where(model.hash_id == hash_id)
                    )

                # C. Bulk Insert NEW Data
                sql_io.bulk_upload(df_copy, conn, model.__table__)

                # D. Update Artifact Manifest (The "DOD" State Record)
                # We use an UPSERT (Merge) here to say "This version is now authoritative"

                # Standard SQL UPSERT pattern (works for SQLite/Postgres)
                # For MSSQL, we might need specific dialect handling, but pure delete/insert works
                # perfectly fine inside a transaction and is portable.

                conn.execute(
                    ArtifactManifest.__table__.delete().where(
                        (ArtifactManifest.hash_id == hash_id)
                        & (ArtifactManifest.target_table == table_name)
                    )
                )

                conn.execute(
                    ArtifactManifest.__table__.insert().values(
                        hash_id=hash_id,
                        target_table=table_name,
                        handler_version=spec.version,
                    )
                )

                total_rows += len(df_copy)

        job_updater._rows_uploaded_in_scope = total_rows
        logger.info(f"[{pipeline_id}] Complete. Total rows: {total_rows}")

    except Exception as e:
        logger.error(f"[{pipeline_id}] Upload failed: {e}", exc_info=True)
        raise
