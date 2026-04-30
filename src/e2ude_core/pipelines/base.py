import logging
import time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from pathlib import Path
from typing import Callable, List, Optional, Type
import sqlalchemy as sa
from e2ude_core.orchestration.spec import JobRunResult
from e2ude_core.db import access as sql_io
from e2ude_core.runtime_files import RuntimeFileSpec
from e2ude_core.db.models import ArtifactManifest, Base, FileHashRegistry
from e2ude_core.db.base_session import DEFAULT_SCHEMA

logger = logging.getLogger(__name__)

MAX_UPLOAD_DEADLOCK_RETRIES = 4
UPLOAD_DEADLOCK_RETRY_DELAY_SECONDS = 0.2


def _qualified_table_name(table_name: str) -> str:
    if DEFAULT_SCHEMA:
        return f"[{DEFAULT_SCHEMA}].[{table_name}]"
    return table_name


def _is_mssql_deadlock(exc: Exception) -> bool:
    message = str(exc).lower()
    return "deadlock victim" in message or "(1205)" in message


def _lock_hash_row(conn: sa.Connection, hash_id: int) -> None:
    """
    Serializes work for the same content hash across parallel workers.

    This is intentionally coarser than a per-table lock: it avoids duplicate
    writes for identical files without taking range locks across many manifest
    keys, which proved deadlock-prone under real MCData fan-out.
    """
    if conn.dialect.name == "mssql":
        qualified = _qualified_table_name(FileHashRegistry.__tablename__)
        conn.execute(
            sa.text(
                f"""
                SELECT id
                FROM {qualified} WITH (UPDLOCK, HOLDLOCK)
                WHERE id = :hash_id
                """
            ),
            {"hash_id": hash_id},
        ).scalar_one()
        return

    conn.execute(
        sa.select(FileHashRegistry.id).where(FileHashRegistry.id == hash_id)
    ).scalar_one()


def _get_manifest_version(
    conn: sa.Connection, hash_id: int, table_name: str
) -> int | None:
    if conn.dialect.name == "mssql":
        qualified = _qualified_table_name(ArtifactManifest.__tablename__)
        row = conn.execute(
            sa.text(
                f"""
                SELECT handler_version
                FROM {qualified} WITH (UPDLOCK, HOLDLOCK)
                WHERE hash_id = :hash_id AND target_table = :target_table
                """
            ),
            {"hash_id": hash_id, "target_table": table_name},
        ).first()
        return None if row is None else row[0]

    return conn.execute(
        sa.select(ArtifactManifest.handler_version).where(
            ArtifactManifest.hash_id == hash_id,
            ArtifactManifest.target_table == table_name,
        )
    ).scalar_one_or_none()


def _upload_single_table(eng: sa.Engine, model, df, hash_id: int, version: int):
    """Upload one model in its own transaction."""
    table_name = model.__tablename__
    for attempt in range(1, MAX_UPLOAD_DEADLOCK_RETRIES + 1):
        try:
            with eng.begin() as conn:
                _lock_hash_row(conn, hash_id)

                current_version = _get_manifest_version(conn, hash_id, table_name)
                if current_version is not None and current_version >= version:
                    logger.info(
                        "Skipping %s for hash %s; artifact already materialized at version %s.",
                        table_name,
                        hash_id,
                        current_version,
                    )
                    return 0

                df_copy = df.copy()
                df_copy["hash_id"] = hash_id

                if hasattr(model, "hash_id"):
                    conn.execute(
                        model.__table__.delete().where(model.hash_id == hash_id)
                    )

                sql_io.bulk_upload(df_copy, conn, model.__table__)

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
        except Exception as exc:
            if attempt < MAX_UPLOAD_DEADLOCK_RETRIES and _is_mssql_deadlock(exc):
                delay = UPLOAD_DEADLOCK_RETRY_DELAY_SECONDS * attempt
                logger.warning(
                    "Deadlock uploading %s for hash %s; retrying in %.1fs (%s/%s).",
                    table_name,
                    hash_id,
                    delay,
                    attempt,
                    MAX_UPLOAD_DEADLOCK_RETRIES - 1,
                )
                time.sleep(delay)
                continue
            raise


def process_file(
    eng: sa.Engine,
    spec: RuntimeFileSpec,
    hash_id: int,
    file_path: Path,
    report_progress: Callable[[str], None],
    target_models: Optional[List[Type[Base]]] = None,
    db_workers: int = 4,
) -> JobRunResult:
    pipeline_id = spec.pipeline_id.value
    try:
        model_to_df_map = spec.parser_func(file_path)
    except Exception:
        logger.error(f"[{pipeline_id}] Parser failed for {file_path}", exc_info=True)
        raise

    payload = []
    models_to_process = (
        set(target_models) if target_models else set(spec.expected_models)
    )

    for model, df in model_to_df_map.items():
        if model in models_to_process and not df.empty:
            payload.append((model, df))

    if not payload:
        return JobRunResult(
            rows_uploaded=0,
            completion_message=f"{pipeline_id} had no rows to upload.",
        )

    total_rows = 0
    errors = []
    report_progress(f"Uploading {len(payload)} tables...")

    with ThreadPoolExecutor(max_workers=db_workers) as executor:
        future_map = {
            executor.submit(
                _upload_single_table, eng, m, d, hash_id, spec.version
            ): m.__tablename__
            for m, d in payload
        }

        done, _ = wait(future_map.keys(), return_when=ALL_COMPLETED)

        for f in done:
            t_name = future_map[f]
            try:
                total_rows += f.result()
            except Exception as e:
                logger.error(f"Failed to upload table {t_name}: {e}")
                errors.append(f"{t_name}: {e}")

    if errors:
        raise RuntimeError(f"Partial upload failure: {errors}")

    logger.info(f"[{pipeline_id}] Complete. Total rows: {total_rows}")
    return JobRunResult(rows_uploaded=total_rows)
