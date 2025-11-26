import logging
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from pathlib import Path
from typing import List, Optional

import sqlalchemy as sa

from e2ude_core.orchestration.managers import JobManager
from e2ude_core.db import access as sql_io
from e2ude_core.registry import HandlerSpec
from e2ude_core.db.models import ArtifactManifest

logger = logging.getLogger(__name__)


def _upload_single_table(
    eng: sa.Engine, 
    model, 
    df, 
    hash_id: int, 
    version: int
):
    """
    Atomic Unit of Work: Replace data for one table and update its manifest.
    """
    table_name = model.__tablename__
    
    # 1. Acquire dedicated connection/transaction
    with eng.begin() as conn:
        # 2. Prep Data
        df_copy = df.copy()
        df_copy["hash_id"] = hash_id

        # 3. Atomic Replace (Data)
        if hasattr(model, "hash_id"):
            conn.execute(
                model.__table__.delete().where(model.hash_id == hash_id)
            )
        sql_io.bulk_upload(df_copy, conn, model.__table__)

        # 4. Update Manifest (State)
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
                handler_version=version,
            )
        )
    
    return len(df)


def process_file(
    eng: sa.Engine,
    spec: HandlerSpec,
    hash_id: int,
    file_path: Path,
    job_updater: JobManager,
    target_tables: Optional[List[str]] = None, # CHANGED: List of targets
    db_workers: int = 4,
):
    pipeline_id = spec.pipeline_id
    logger.info(f"[{pipeline_id}] Processing HashID {hash_id}. Targets: {len(target_tables) if target_tables else 'ALL'}")

    # 1. Parse (Once per file)
    try:
        model_to_df_map = spec.parser_func(file_path)
    except Exception:
        logger.error(f"[{pipeline_id}] Parser failed for {file_path}", exc_info=True)
        raise

    # 2. Filter Payload
    payload = []
    # Pre-compute allowed tables for O(1) lookup
    valid_models = {m.__tablename__: m for m in spec.expected_models}
    
    # Determine what to process
    if target_tables:
        # Process only requested missing tables
        tables_to_process = set(target_tables)
    else:
        # Process everything (default)
        tables_to_process = set(valid_models.keys())

    for model, df in model_to_df_map.items():
        t_name = model.__tablename__
        
        # Skip if not in our target list
        if t_name not in tables_to_process:
            continue
            
        if df.empty:
            continue
        
        payload.append((model, df))

    if not payload:
        job_updater._rows_uploaded_in_scope = 0
        return

    # 3. Parallel Atomic Upload
    total_rows = 0
    row_count_sum = sum(len(item[1]) for item in payload)
    job_updater.mark_running(f"Uploading {row_count_sum} rows into {len(payload)} tables...")

    errors = []

    with ThreadPoolExecutor(max_workers=db_workers) as executor:
        future_to_table = {
            executor.submit(
                _upload_single_table,
                eng, model, df, hash_id, spec.version
            ): model.__tablename__
            for model, df in payload
        }
        
        # CHANGED: Wait for ALL to complete. 
        # Even if Table A fails, we want Table B to succeed and update its manifest.
        done, _ = wait(future_to_table.keys(), return_when=ALL_COMPLETED)
        
        for f in done:
            t_name = future_to_table[f]
            try:
                rows = f.result()
                total_rows += rows
            except Exception as e:
                logger.error(f"Failed to upload table {t_name}: {e}")
                errors.append(f"{t_name}: {str(e)}")

    # 4. Final Status Check
    job_updater._rows_uploaded_in_scope = total_rows
    
    if errors:
        # If any table failed, we raise an exception so the Job is marked as ERROR
        # However, successful tables HAVE been committed (Partial Success).
        error_msg = f"Partial Failure ({len(errors)} tables failed): {'; '.join(errors)}"
        raise RuntimeError(error_msg)

    logger.info(f"[{pipeline_id}] Complete. Total rows: {total_rows}")