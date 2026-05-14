import argparse
import json
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import inspect

from e2ude_core.config import settings
from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.base_session import DEFAULT_SCHEMA
from e2ude_core.db.models import (
    ProcessingJob,
    ProcessingSession,
    StatusEnum,
)
from e2ude_core.db.setup import initialize_database, register_archives_bulk
from e2ude_core.logging_conf import setup_logging
from e2ude_core.orchestration.workflow import process_staged_archive
from e2ude_core.services.zip_io import UnzipContext


def _qualified_table_name(schema_name: str | None, table_name: str) -> str:
    if schema_name:
        return f"[{schema_name}].[{table_name}]"
    return table_name


def _collect_counts(eng: sa.Engine, schema_name: str | None) -> dict[str, int]:
    table_names = inspect(eng).get_table_names(schema=schema_name)
    counts: dict[str, int] = {}
    with eng.connect() as conn:
        for table_name in sorted(table_names):
            qualified = _qualified_table_name(schema_name, table_name)
            counts[table_name] = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {qualified}")
            ).scalar_one()
    return counts


def _collect_run_status(eng: sa.Engine, archive_id: int) -> dict[str, object]:
    with eng.connect() as conn:
        session_row = conn.execute(
            sa.select(
                ProcessingSession.id,
                ProcessingSession.status,
                ProcessingSession.start_time,
                ProcessingSession.end_time,
            )
            .join(ProcessingJob, ProcessingJob.session_id == ProcessingSession.id)
            .where(ProcessingJob.archive_id == archive_id)
            .order_by(ProcessingSession.id.desc())
            .limit(1)
        ).first()

        if session_row is None:
            return {
                "session_id": None,
                "session_status": None,
                "error_jobs": 0,
            }

        error_jobs = conn.execute(
            sa.select(sa.func.count())
            .select_from(ProcessingJob.__table__)
            .where(
                ProcessingJob.session_id == session_row.id,
                ProcessingJob.status == StatusEnum.ERROR,
            )
        ).scalar_one()

    return {
        "session_id": session_row.id,
        "session_status": session_row.status.value,
        "session_start_time": str(session_row.start_time),
        "session_end_time": None
        if session_row.end_time is None
        else str(session_row.end_time),
        "error_jobs": error_jobs,
    }


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run a real single-zip E2E load into the currently configured database/schema."
        )
    )
    parser.add_argument("zip_path", type=Path, help="Path to a TransportRSM fixture zip")
    args = parser.parse_args()

    zip_path = args.zip_path.expanduser().resolve()
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    setup_logging(settings)
    eng = get_engine(settings.database)

    try:
        initialize_database(eng, reset_tables=False)
        archive_map = register_archives_bulk(eng, [zip_path])
        if zip_path not in archive_map:
            raise ValueError(
                "Zip filename must match the expected archive pattern, for example "
                "'169871_20231107_024218_987_TransportRSM.fpkg.e2d.zip'."
            )
        archive_id = archive_map[zip_path]

        with UnzipContext(zip_path) as ctx:
            process_staged_archive(
                eng=eng,
                archive_id=archive_id,
                staged_path=Path(ctx.temp_dir),
                context=EtlContext.capture(),
            )

        table_counts = _collect_counts(eng, DEFAULT_SCHEMA)
        materialized_tables = {
            table_name: count
            for table_name, count in table_counts.items()
            if table_name.startswith("rsmdata_") and count > 0
        }
        payload = {
            "database_type": settings.database.type,
            "schema_name": DEFAULT_SCHEMA,
            "zip_path": str(zip_path),
            "archive_id": archive_id,
            "run_status": _collect_run_status(eng, archive_id),
            "table_counts": table_counts,
            "materialized_tables": materialized_tables,
        }
        print(json.dumps(payload, indent=2))
        if payload["run_status"]["error_jobs"] > 0:
            raise SystemExit(1)
    finally:
        eng.dispose()


if __name__ == "__main__":
    main()
