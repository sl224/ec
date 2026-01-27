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


def _upload_single_table(eng: sa.Engine, model, df, hash_id: int, version: int):
    """Worker: Uploads one table in its own transaction."""
    table_name = model.__tablename__
    with eng.begin() as conn:
        df_copy = df.copy()
        df_copy["hash_id"] = hash_id

        if hasattr(model, "hash_id"):
            conn.execute(model.__table__.delete().where(model.hash_id == hash_id))

        sql_io.bulk_upload(df_copy, conn, model.__table__)

        conn.execute(
            ArtifactManifest.__table__.delete().where(
                (ArtifactManifest.hash_id == hash_id)
                & (ArtifactManifest.target_table == table_name)
            )
        )
        conn.execute(
            ArtifactManifest.__table__.insert().values(
                hash_id=hash_id, target_table=table_name, handler_version=version
            )
        )
    return len(df)


def process_file(
    eng: sa.Engine,
    spec: HandlerSpec,
    hash_id: int,
    file_path: Path,
    job_updater: JobManager,
    target_tables: Optional[List[str]] = None,
    db_workers: int = 4,
):
    pipeline_id = spec.pipeline_id
    # 1. Parse
    try:
        model_to_df_map = spec.parser_func(file_path)
    except Exception:
        logger.error(f"[{pipeline_id}] Parser failed for {file_path}", exc_info=True)
        raise

    # 2. Filter
    payload = []
    valid_models = {m.__tablename__: m for m in spec.expected_models}
    tables_to_process = (
        set(target_tables) if target_tables else set(valid_models.keys())
    )

    for model, df in model_to_df_map.items():
        if model.__tablename__ in tables_to_process and not df.empty:
            payload.append((model, df))

    if not payload:
        job_updater._rows_uploaded_in_scope = 0
        return

    # 3. Parallel Upload
    total_rows = 0
    errors = []
    job_updater.mark_running(f"Uploading {len(payload)} tables...")

    with ThreadPoolExecutor(max_workers=db_workers) as executor:
        future_map = {
            executor.submit(
                _upload_single_table, eng, m, d, hash_id, spec.version
            ): m.__tablename__
            for m, d in payload
        }

        # Wait for ALL tables. Don't stop if one fails.
        done, _ = wait(future_map.keys(), return_when=ALL_COMPLETED)

        for f in done:
            t_name = future_map[f]
            try:
                total_rows += f.result()
            except Exception as e:
                logger.error(f"Failed to upload table {t_name}: {e}")
                errors.append(f"{t_name}: {e}")

    job_updater._rows_uploaded_in_scope = total_rows
    if errors:
        raise RuntimeError(f"Partial upload failure: {errors}")

    logger.info(f"[{pipeline_id}] Complete. Total rows: {total_rows}")
