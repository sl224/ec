import logging
import time
from pathlib import Path
from typing import Callable, Sequence, Type

import sqlalchemy as sa

from e2ude_core.db import access as sql_io
from e2ude_core.db.base_session import DEFAULT_SCHEMA
from e2ude_core.db.models import ArtifactManifest, Base, FileHashRegistry
from e2ude_core.orchestration.runs import JobRunResult
from e2ude_core.runtime_files import RuntimeFileSpec

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
                SELECT parser_version
                FROM {qualified} WITH (UPDLOCK, HOLDLOCK)
                WHERE hash_id = :hash_id AND target_table = :target_table
                """
            ),
            {"hash_id": hash_id, "target_table": table_name},
        ).first()
        return None if row is None else row[0]

    return conn.execute(
        sa.select(ArtifactManifest.parser_version).where(
            ArtifactManifest.hash_id == hash_id,
            ArtifactManifest.target_table == table_name,
        )
    ).scalar_one_or_none()


def _column_can_fill_itself(column) -> bool:
    has_default = column.default is not None or column.server_default is not None
    is_identity = bool(column.primary_key and column.autoincrement is not False)
    return has_default or is_identity


def _required_parser_columns(model: Type[Base]) -> set[str]:
    return {
        column.name
        for column in model.__table__.columns
        if column.name != "hash_id"
        and not column.nullable
        and not _column_can_fill_itself(column)
    }


def _validate_parser_output(spec: RuntimeFileSpec, payload) -> None:
    missing_models = [
        model.__name__ for model in spec.expected_models if model not in payload
    ]
    if missing_models:
        raise RuntimeError(
            f"{spec.pipeline_id} parser did not return expected outputs: "
            f"{', '.join(sorted(missing_models))}"
        )

    unknown_models = [
        getattr(model, "__name__", str(model))
        for model in payload
        if model not in spec.expected_models
    ]
    if unknown_models:
        raise RuntimeError(
            f"{spec.pipeline_id} parser returned unknown outputs: "
            f"{', '.join(sorted(unknown_models))}"
        )

    for model in spec.expected_models:
        df = payload[model]
        missing_columns = _required_parser_columns(model) - set(df.columns)
        if missing_columns:
            raise RuntimeError(
                f"{spec.pipeline_id} parser output for {model.__tablename__} "
                f"is missing required columns: {', '.join(sorted(missing_columns))}"
            )


def _replace_artifacts(
    conn: sa.Connection,
    *,
    spec: RuntimeFileSpec,
    hash_id: int,
    payload,
    target_models: tuple[Type[Base], ...],
    force: bool,
) -> dict[str, int]:
    _lock_hash_row(conn, hash_id)
    table_rows: dict[str, int] = {}
    version = spec.version or 0

    for model in target_models:
        table_name = model.__tablename__
        current_version = _get_manifest_version(conn, hash_id, table_name)
        if not force and current_version is not None and current_version >= version:
            table_rows[table_name] = 0
            continue

        df = payload[model].copy()
        df["hash_id"] = hash_id

        conn.execute(model.__table__.delete().where(model.hash_id == hash_id))
        sql_io.bulk_upload(df, conn, model.__table__)

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
                parser_version=version,
                row_count=len(payload[model]),
            )
        )
        table_rows[table_name] = len(payload[model])

    return table_rows


def process_file(
    eng: sa.Engine,
    spec: RuntimeFileSpec,
    hash_id: int,
    file_path: Path,
    report_progress: Callable[[str], None],
    target_models: Sequence[Type[Base]] | None = None,
    force: bool = False,
) -> JobRunResult:
    pipeline_id = spec.pipeline_id.value if spec.pipeline_id else spec.file_type.value
    try:
        payload = spec.parser_func(file_path)
    except Exception:
        logger.error("[%s] Parser failed for %s", pipeline_id, file_path, exc_info=True)
        raise

    _validate_parser_output(spec, payload)
    models_to_process = tuple(target_models or spec.expected_models)
    unknown_targets = [
        model.__name__
        for model in models_to_process
        if model not in spec.expected_models
    ]
    if unknown_targets:
        raise RuntimeError(
            f"{pipeline_id} was asked to write unknown outputs: "
            f"{', '.join(sorted(unknown_targets))}"
        )

    report_progress(f"Uploading {len(models_to_process)} tables...")
    for attempt in range(1, MAX_UPLOAD_DEADLOCK_RETRIES + 1):
        try:
            with eng.begin() as conn:
                table_rows = _replace_artifacts(
                    conn,
                    spec=spec,
                    hash_id=hash_id,
                    payload=payload,
                    target_models=models_to_process,
                    force=force,
                )
            total_rows = sum(table_rows.values())
            logger.info("[%s] Complete. Total rows: %s", pipeline_id, total_rows)
            return JobRunResult(
                rows_uploaded=total_rows,
                table_rows=table_rows,
                completion_message=f"{pipeline_id} processed successfully",
            )
        except Exception as exc:
            if attempt < MAX_UPLOAD_DEADLOCK_RETRIES and _is_mssql_deadlock(exc):
                delay = UPLOAD_DEADLOCK_RETRY_DELAY_SECONDS * attempt
                logger.warning(
                    "Deadlock uploading %s for hash %s; retrying in %.1fs (%s/%s).",
                    pipeline_id,
                    hash_id,
                    delay,
                    attempt,
                    MAX_UPLOAD_DEADLOCK_RETRIES - 1,
                )
                time.sleep(delay)
                continue
            raise

    raise RuntimeError(f"{pipeline_id} upload retry loop exited unexpectedly")
