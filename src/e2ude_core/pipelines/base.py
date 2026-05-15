import logging
import time
from pathlib import Path
from typing import Callable, Sequence, Type

import sqlalchemy as sa

from e2ude_core.db import access as sql_io
from e2ude_core.db.base_session import DEFAULT_SCHEMA
from e2ude_core.db.models import ArtifactManifest, Base
from e2ude_core.orchestration.runs import JobRunResult
from e2ude_core.runtime_files import RuntimeFileSpec, artifact_key_for

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


def _lock_content_hash(conn: sa.Connection, content_hash: bytes) -> None:
    if conn.dialect.name != "mssql":
        return

    conn.execute(
        sa.text(
            """
            EXEC sp_getapplock
                @Resource = :resource,
                @LockMode = 'Exclusive',
                @LockOwner = 'Transaction',
                @LockTimeout = 60000
            """
        ),
        {"resource": f"e2ude_content_hash_{content_hash.hex()}"},
    )


def _get_manifest_state(
    conn: sa.Connection, content_hash: bytes, artifact_key: str
) -> tuple[int, str] | None:
    if conn.dialect.name == "mssql":
        qualified = _qualified_table_name(ArtifactManifest.__tablename__)
        row = conn.execute(
            sa.text(
                f"""
                SELECT parser_version, target_table
                FROM {qualified} WITH (UPDLOCK, HOLDLOCK)
                WHERE content_hash = :content_hash AND artifact_key = :artifact_key
                """
            ),
            {"content_hash": content_hash, "artifact_key": artifact_key},
        ).first()
        return None if row is None else (row[0], row[1])

    row = conn.execute(
        sa.select(ArtifactManifest.parser_version, ArtifactManifest.target_table).where(
            ArtifactManifest.content_hash == content_hash,
            ArtifactManifest.artifact_key == artifact_key,
        )
    ).first()
    return None if row is None else (row.parser_version, row.target_table)


def _column_can_fill_itself(column) -> bool:
    has_default = column.default is not None or column.server_default is not None
    is_identity = bool(column.primary_key and column.autoincrement is not False)
    return has_default or is_identity


def _required_parser_columns(model: Type[Base]) -> set[str]:
    return {
        column.name
        for column in model.__table__.columns
        if column.name != "content_hash"
        and not column.nullable
        and not _column_can_fill_itself(column)
    }


def _validate_parser_output(spec: RuntimeFileSpec, payload) -> None:
    missing_models = [
        model.__name__ for model in spec.expected_models if model not in payload
    ]
    if missing_models:
        raise RuntimeError(
            f"{spec.parser_id} parser did not return expected outputs: "
            f"{', '.join(sorted(missing_models))}"
        )

    unknown_models = [
        getattr(model, "__name__", str(model))
        for model in payload
        if model not in spec.expected_models
    ]
    if unknown_models:
        raise RuntimeError(
            f"{spec.parser_id} parser returned unknown outputs: "
            f"{', '.join(sorted(unknown_models))}"
        )

    for model in spec.expected_models:
        df = payload[model]
        missing_columns = _required_parser_columns(model) - set(df.columns)
        if missing_columns:
            raise RuntimeError(
                f"{spec.parser_id} parser output for {model.__tablename__} "
                f"is missing required columns: {', '.join(sorted(missing_columns))}"
            )


def _replace_artifacts(
    conn: sa.Connection,
    *,
    spec: RuntimeFileSpec,
    content_hash: bytes,
    payload,
    target_models: tuple[Type[Base], ...],
    force: bool,
) -> dict[str, int]:
    _lock_content_hash(conn, content_hash)
    table_rows: dict[str, int] = {}
    version = spec.version or 0

    for model in target_models:
        artifact_key = artifact_key_for(spec, model)
        table_name = model.__tablename__
        current_state = _get_manifest_state(conn, content_hash, artifact_key)
        if (
            not force
            and current_state is not None
            and current_state[0] >= version
            and current_state[1] == table_name
        ):
            table_rows[table_name] = 0
            continue

        df = payload[model].copy()
        df["content_hash"] = content_hash

        conn.execute(model.__table__.delete().where(model.content_hash == content_hash))
        sql_io.bulk_upload(df, conn, model.__table__)

        conn.execute(
            ArtifactManifest.__table__.delete().where(
                (ArtifactManifest.content_hash == content_hash)
                & (ArtifactManifest.artifact_key == artifact_key)
            )
        )
        conn.execute(
            ArtifactManifest.__table__.insert().values(
                content_hash=content_hash,
                artifact_key=artifact_key,
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
    content_hash: bytes,
    file_path: Path,
    report_progress: Callable[[str], None],
    target_models: Sequence[Type[Base]] | None = None,
    force: bool = False,
) -> JobRunResult:
    pipeline_id = spec.parser_id or spec.file_type.value
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
                    content_hash=content_hash,
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
                    content_hash.hex(),
                    delay,
                    attempt,
                    MAX_UPLOAD_DEADLOCK_RETRIES - 1,
                )
                time.sleep(delay)
                continue
            raise

    raise RuntimeError(f"{pipeline_id} upload retry loop exited unexpectedly")
