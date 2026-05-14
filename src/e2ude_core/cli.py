from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Iterable, Sequence

import pandas as pd

from e2ude_core.db.schema_safety import schema_classification, validate_schema_name


MSSQL_TARGET_SCHEMAS = {
    "mssql-dev": "e2ude_core_dev",
    "mssql-prod": "e2ude_core",
}
ENV_SCHEMAS = {
    "dev": "e2ude_core_dev",
    "prod": "e2ude_core",
}
AUDIT_TABLES = {"processing_sessions", "processing_jobs"}


@dataclass(frozen=True)
class TargetInfo:
    backend: str
    command: str
    server: str | None = None
    database: str | None = None
    schema: str | None = None
    schema_class: str | None = None
    sqlite_path: str | None = None


@dataclass(frozen=True)
class ParserWorkItem:
    archive_id: int
    source_path: Path
    file_id: int
    hash_id: int
    relative_path: str
    target_models: tuple
    handler_version: int


def _parser_id(spec) -> str:
    return spec.pipeline_id.value


def _local_hints(spec) -> tuple[str, ...]:
    hints: set[str] = set()
    for pattern in spec.match_patterns:
        name = PurePosixPath(pattern).name
        if not name:
            continue
        if any(char in name for char in "*?["):
            stripped = name.replace("*", "").strip("_")
            if stripped and not any(char in stripped for char in "?["):
                hints.add(stripped)
            continue
        hints.add(name)
    return tuple(sorted(hints, key=str.casefold))


def _handled_specs():
    from e2ude_core.runtime_files import HANDLED_FILE_SPECS

    return tuple(HANDLED_FILE_SPECS)


def _apply_target_env(args, *, require_db: bool) -> None:
    config_path = getattr(args, "config", None)
    if config_path is not None:
        os.environ["E2UDE_CONFIG_PATH"] = str(Path(config_path).expanduser().resolve())

    target = getattr(args, "target", None)
    env_name = getattr(args, "env", None)
    sqlite_path = getattr(args, "sqlite", None)
    schema_name = getattr(args, "schema", None)

    if sum(bool(value) for value in (target, env_name, sqlite_path)) > 1:
        raise SystemExit("Choose only one of --env, --target, or --sqlite.")
    if sqlite_path and schema_name:
        raise SystemExit("--schema applies only to MSSQL targets.")
    if env_name and schema_name:
        raise SystemExit(
            "Choose --env for a shared schema or --schema for a custom schema."
        )
    if (
        require_db
        and not target
        and not env_name
        and not sqlite_path
        and not schema_name
    ):
        raise SystemExit(
            "Choose --env dev, --env prod, --schema NAME, --target mssql-dev, "
            "--target mssql-prod, or --sqlite."
        )

    if sqlite_path:
        db_path = Path(sqlite_path).expanduser().resolve()
        os.environ["E2UDE_DATABASE__TYPE"] = "sqlite3"
        os.environ["E2UDE_DATABASE__DB_LOCATION"] = str(db_path)
        os.environ["E2UDE_DATABASE__IN_MEMORY"] = "false"
        os.environ.pop("E2UDE_DATABASE__SCHEMA_NAME", None)
        return

    if target or env_name or schema_name:
        if env_name:
            schema = ENV_SCHEMAS[env_name]
        elif target:
            schema = schema_name or MSSQL_TARGET_SCHEMAS[target]
        else:
            schema = schema_name
        validate_schema_name(schema)
        os.environ["E2UDE_DATABASE__TYPE"] = "mssql"
        os.environ["E2UDE_DATABASE__SCHEMA_NAME"] = schema


def _resolve_schema_ref(value: str) -> str:
    return validate_schema_name(ENV_SCHEMAS.get(value, value))


def _apply_schema_command_env(args, schema_name: str | None = None) -> None:
    config_path = getattr(args, "config", None)
    if config_path is not None:
        os.environ["E2UDE_CONFIG_PATH"] = str(Path(config_path).expanduser().resolve())
    os.environ["E2UDE_DATABASE__TYPE"] = "mssql"
    if schema_name is not None:
        os.environ["E2UDE_DATABASE__SCHEMA_NAME"] = schema_name
    elif getattr(args, "target", None):
        os.environ["E2UDE_DATABASE__SCHEMA_NAME"] = MSSQL_TARGET_SCHEMAS[args.target]


def _target_info(command: str) -> TargetInfo:
    from e2ude_core.config import settings

    if settings.database.type == "mssql":
        schema = settings.database.schema_name
        return TargetInfo(
            backend="mssql",
            command=command,
            server=settings.database.server_name,
            database=settings.database.db_name,
            schema=schema,
            schema_class=schema_classification(schema).value,
        )

    return TargetInfo(
        backend="sqlite",
        command=command,
        sqlite_path=settings.database.db_location,
    )


def _print_target(info: TargetInfo) -> None:
    print("Target")
    print(f"  backend   {info.backend}")
    if info.backend == "mssql":
        print(f"  server    {info.server}")
        print(f"  database  {info.database}")
        print(f"  schema    {info.schema}")
        print(f"  class     {info.schema_class}")
    else:
        print(f"  file      {info.sqlite_path}")
        print("  schema    n/a")
    print(f"  command   {info.command}")


def _resolve_parser(selection: str, specs):
    wanted = selection.casefold()
    exact = []
    for spec in specs:
        names = {_parser_id(spec), spec.file_type.value, *(_local_hints(spec))}
        if wanted in {name.casefold() for name in names}:
            exact.append(spec)
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        choices = ", ".join(sorted(_parser_id(spec) for spec in exact))
        raise SystemExit(f"{selection!r} is ambiguous: {choices}")

    prefix = [
        spec
        for spec in specs
        if _parser_id(spec).casefold().startswith(wanted)
        or spec.file_type.value.casefold().startswith(wanted)
    ]
    if len(prefix) == 1:
        return prefix[0]
    if len(prefix) > 1:
        choices = ", ".join(sorted(_parser_id(spec) for spec in prefix))
        raise SystemExit(f"{selection!r} matched multiple parsers: {choices}")

    choices = ", ".join(sorted(_parser_id(spec) for spec in specs))
    raise SystemExit(f"Unknown parser {selection!r}. Available parsers: {choices}")


def _resolve_parser_from_file(file_path: Path, explicit: str | None, specs):
    if explicit:
        return _resolve_parser(explicit, specs), "explicit"

    from e2ude_core.services.file_catalog import detect_file_type

    by_file_type = {spec.file_type: spec for spec in specs}
    candidates = [
        Path(*file_path.parts[index:]) for index in range(len(file_path.parts))
    ]
    for candidate in candidates:
        detected = detect_file_type(candidate)
        spec = by_file_type.get(detected)
        if spec is not None:
            return spec, candidate.as_posix()

    file_name = file_path.name.casefold()
    hint_matches = [
        spec
        for spec in specs
        if file_name in {hint.casefold() for hint in _local_hints(spec)}
    ]
    if len(hint_matches) == 1:
        return hint_matches[0], "local filename"
    if len(hint_matches) > 1:
        choices = ", ".join(sorted(_parser_id(spec) for spec in hint_matches))
        raise SystemExit(f"{file_path.name!r} matched multiple parsers: {choices}")

    suggestions = "\n".join(
        f"  --as {_parser_id(spec):<16} hints: {', '.join(_local_hints(spec)) or '-'}"
        for spec in specs
    )
    raise SystemExit(
        f"Could not infer parser for {file_path}.\nTry one of:\n{suggestions}"
    )


def _preview_records(df: pd.DataFrame, head_rows: int) -> list[dict[str, object]]:
    preview_df = df.head(head_rows).astype(object)
    preview_df = preview_df.where(pd.notna(preview_df), None)
    return preview_df.to_dict(orient="records")


def _print_table(headers: list[str], rows: list[list[object]]) -> None:
    text_rows = [[str(value) for value in row] for row in rows]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in text_rows))
        for index in range(len(headers))
    ]
    print(
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    )
    print("  ".join("-" * width for width in widths))
    for row in text_rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _parser_counts(eng, specs) -> dict[str, dict[str, int]]:
    import sqlalchemy as sa

    from e2ude_core.db.models import ArtifactManifest, FileMetadata

    counts: dict[str, dict[str, int]] = {}
    with eng.connect() as conn:
        for spec in specs:
            hash_rows = conn.execute(
                sa.select(FileMetadata.hash_id).where(
                    FileMetadata.file_type == spec.file_type.value
                )
            ).fetchall()
            hash_ids = {row.hash_id for row in hash_rows}
            target_tables = [model.__tablename__ for model in spec.expected_models]
            complete = 0
            rows_uploaded = 0
            if hash_ids and target_tables:
                artifact_rows = conn.execute(
                    sa.select(
                        ArtifactManifest.hash_id,
                        ArtifactManifest.target_table,
                        ArtifactManifest.row_count,
                    ).where(
                        ArtifactManifest.hash_id.in_(hash_ids),
                        ArtifactManifest.target_table.in_(target_tables),
                        ArtifactManifest.handler_version >= spec.version,
                    )
                ).fetchall()
                by_hash: dict[int, set[str]] = {}
                for row in artifact_rows:
                    by_hash.setdefault(row.hash_id, set()).add(row.target_table)
                    rows_uploaded += row.row_count or 0
                complete = sum(
                    1
                    for table_set in by_hash.values()
                    if len(table_set) == len(target_tables)
                )

            counts[_parser_id(spec)] = {
                "files": len(hash_rows),
                "hashes": len(hash_ids),
                "complete": complete,
                "missing": max(len(hash_ids) - complete, 0),
                "rows": rows_uploaded,
            }
    return counts


def cmd_parsers(args) -> int:
    if args.counts:
        _apply_target_env(args, require_db=True)
        from e2ude_core.config import settings
        from e2ude_core.db.access import get_engine

        specs = _handled_specs()
        _print_target(_target_info("parsers --counts"))
        eng = get_engine(settings.database)
        try:
            counts = _parser_counts(eng, specs)
        finally:
            eng.dispose()
    else:
        specs = _handled_specs()
        counts = {}

    headers = ["parser", "file_type", "version", "hints", "patterns", "outputs"]
    if args.counts:
        headers.extend(["files", "hashes", "complete", "missing/stale", "rows"])

    rows = []
    for spec in sorted(specs, key=lambda item: _parser_id(item)):
        row = [
            _parser_id(spec),
            spec.file_type.value,
            spec.version,
            ", ".join(_local_hints(spec)) or "-",
            "; ".join(spec.match_patterns),
            ", ".join(model.__tablename__ for model in spec.expected_models),
        ]
        if args.counts:
            parser_counts = counts[_parser_id(spec)]
            row.extend(
                [
                    parser_counts["files"],
                    parser_counts["hashes"],
                    parser_counts["complete"],
                    parser_counts["missing"],
                    parser_counts["rows"],
                ]
            )
        rows.append(row)

    _print_table(headers, rows)
    return 0


def cmd_parser_status(args) -> int:
    _apply_target_env(args, require_db=True)
    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine

    specs = _handled_specs()
    if getattr(args, "parser", None):
        specs = (_resolve_parser(args.parser, specs),)

    _print_target(_target_info("parser status"))
    eng = get_engine(settings.database)
    try:
        counts = _parser_counts(eng, specs)
    finally:
        eng.dispose()

    rows = []
    for spec in sorted(specs, key=lambda item: _parser_id(item)):
        parser_counts = counts[_parser_id(spec)]
        rows.append(
            [
                _parser_id(spec),
                spec.version,
                parser_counts["files"],
                parser_counts["hashes"],
                parser_counts["complete"],
                parser_counts["missing"],
                parser_counts["rows"],
            ]
        )

    _print_table(
        ["parser", "version", "files", "hashes", "complete", "missing/stale", "rows"],
        rows,
    )
    return 0


def cmd_preview(args) -> int:
    specs = _handled_specs()
    file_path = args.file_path.expanduser().resolve()
    if not file_path.is_file():
        raise SystemExit(f"Input file not found: {file_path}")
    if args.head <= 0:
        raise SystemExit(f"--head must be positive, got {args.head}")

    spec, selection_source = _resolve_parser_from_file(file_path, args.as_parser, specs)
    payload = spec.parser_func(file_path)

    output_tables = []
    for model in spec.expected_models:
        df = payload.get(model)
        if df is None:
            output_tables.append(
                {
                    "model": model.__name__,
                    "table": model.__tablename__,
                    "rows": None,
                    "columns": [],
                    "preview": [],
                }
            )
            continue
        output_tables.append(
            {
                "model": model.__name__,
                "table": model.__tablename__,
                "rows": int(len(df)),
                "columns": list(df.columns),
                "preview": _preview_records(df, args.head),
            }
        )

    print(
        json.dumps(
            {
                "file_path": str(file_path),
                "selected_parser": _parser_id(spec),
                "selected_file_type": spec.file_type.value,
                "pipeline_id": _parser_id(spec),
                "selection_source": selection_source,
                "handler_version": spec.version,
                "tables": output_tables,
            },
            indent=2,
            default=str,
        )
    )
    return 0


def _fetch_artifact_versions(
    conn, hash_ids: Iterable[int]
) -> dict[int, dict[str, int]]:
    import sqlalchemy as sa

    from e2ude_core.db.models import ArtifactManifest

    hash_id_list = sorted(set(hash_ids))
    if not hash_id_list:
        return {}

    versions: dict[int, dict[str, int]] = {}
    rows = conn.execute(
        sa.select(
            ArtifactManifest.hash_id,
            ArtifactManifest.target_table,
            ArtifactManifest.handler_version,
        ).where(ArtifactManifest.hash_id.in_(hash_id_list))
    )
    for row in rows:
        current = versions.setdefault(row.hash_id, {}).get(row.target_table, 0)
        versions[row.hash_id][row.target_table] = max(current, row.handler_version)
    return versions


def _plan_parser_work(
    eng,
    spec,
    *,
    limit: int | None = None,
    failed_only: bool = False,
    force: bool = False,
):
    import sqlalchemy as sa

    from e2ude_core.db.models import (
        ArchiveMetadata,
        FileMetadata,
        ProcessingJob,
        StatusEnum,
    )

    with eng.connect() as conn:
        stmt = (
            sa.select(
                ArchiveMetadata.id.label("archive_id"),
                ArchiveMetadata.source_path,
                FileMetadata.id.label("file_id"),
                FileMetadata.hash_id,
                FileMetadata.relative_path,
            )
            .select_from(FileMetadata)
            .join(ArchiveMetadata, ArchiveMetadata.id == FileMetadata.archive_id)
            .where(
                ArchiveMetadata.is_present == sa.true(),
                ArchiveMetadata.completed_scan_version
                >= ArchiveMetadata.required_scan_version,
                FileMetadata.file_type == spec.file_type.value,
            )
        )
        if failed_only:
            stmt = stmt.join(ProcessingJob, ProcessingJob.file_id == FileMetadata.id)
            stmt = stmt.where(
                ProcessingJob.pipeline_id == _parser_id(spec),
                ProcessingJob.status == StatusEnum.ERROR,
            )

        rows = conn.execute(stmt).fetchall()
        artifact_versions = _fetch_artifact_versions(
            conn, (row.hash_id for row in rows)
        )

    representatives = {}
    for row in sorted(
        rows, key=lambda item: (item.hash_id, item.archive_id, item.file_id)
    ):
        representatives.setdefault(row.hash_id, row)

    items: list[ParserWorkItem] = []
    for row in representatives.values():
        current_versions = artifact_versions.get(row.hash_id, {})
        if force:
            missing_models = spec.expected_models
        else:
            missing_models = tuple(
                model
                for model in spec.expected_models
                if current_versions.get(model.__tablename__, 0) < spec.version
            )
        if not missing_models:
            continue
        items.append(
            ParserWorkItem(
                archive_id=row.archive_id,
                source_path=Path(row.source_path),
                file_id=row.file_id,
                hash_id=row.hash_id,
                relative_path=row.relative_path,
                target_models=missing_models,
                handler_version=spec.version,
            )
        )

    items.sort(
        key=lambda item: (str(item.source_path), item.relative_path, item.hash_id)
    )
    if limit is not None:
        items = items[:limit]
    return items, len(rows), len(representatives)


def _print_parser_plan(
    spec, items: list[ParserWorkItem], file_rows: int, hashes: int
) -> None:
    print(f"Parser      {_parser_id(spec)}")
    print(f"File type   {spec.file_type.value}")
    print(f"Version     {spec.version}")
    print(f"Files       {file_rows}")
    print(f"Hashes      {hashes}")
    print(f"Pending     {len(items)}")
    if not items:
        print("No pending parser artifacts found.")
        return

    rows = [
        [
            item.archive_id,
            item.file_id,
            item.hash_id,
            item.relative_path,
            ", ".join(model.__tablename__ for model in item.target_models),
        ]
        for item in items[:20]
    ]
    _print_table(["archive", "file", "hash", "relative_path", "missing_outputs"], rows)
    if len(items) > 20:
        print(f"... {len(items) - 20} more")


def _copy_failure_artifact(
    path: Path, failure_dir: Path, item: ParserWorkItem
) -> Path | None:
    if not path.exists():
        return None
    failure_dir.mkdir(parents=True, exist_ok=True)
    target = failure_dir / (
        f"archive_{item.archive_id}_file_{item.file_id}_hash_{item.hash_id}_{path.name}"
    )
    shutil.copy2(path, target)
    return target


def _stage_archive(
    zip_path: Path, local_dir: Path, relative_paths: Sequence[str]
) -> None:
    from e2ude_core.services.zip_io import extract_transport_zip

    if local_dir.exists():
        shutil.rmtree(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    temp_zip = local_dir / "temp_source.zip"
    shutil.copyfile(zip_path, temp_zip)
    extract_transport_zip(temp_zip, local_dir, active_patterns=list(relative_paths))
    temp_zip.unlink()


def _lookup_processing_job_id(eng, session_id: int, spec) -> int | None:
    import sqlalchemy as sa

    from e2ude_core.db.models import ProcessingJob

    with eng.connect() as conn:
        return conn.execute(
            sa.select(ProcessingJob.id).where(
                ProcessingJob.session_id == session_id,
                ProcessingJob.pipeline_id == spec.pipeline_id.value,
                ProcessingJob.file_id == spec.file_id,
                ProcessingJob.dataset_key == spec.target_key,
            )
        ).scalar_one_or_none()


def _run_parser_items(args, eng, spec, items: list[ParserWorkItem]) -> int:
    from e2ude_core.config import settings
    from e2ude_core.context import EtlContext
    from e2ude_core.orchestration.runs import (
        create_processing_session,
        finalize_processing_session,
        run_processing_job,
    )
    from e2ude_core.orchestration.spec import JobSpec, build_job_target
    from e2ude_core.pipelines.base import process_file

    staging_root = Path(args.staging_root or settings.paths.staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)
    failure_dir = Path(args.failure_dir or staging_root / "failures")
    context = EtlContext.capture()

    total_rows = 0
    failed = 0
    for item in items:
        session_id = None
        job_spec = None
        session_failed = False
        stage_dir = staging_root / (
            f"cli_{_parser_id(spec)}_{item.archive_id}_{item.file_id}"
        )
        full_path = stage_dir / item.relative_path
        try:
            session_id = create_processing_session(eng, item.archive_id, context)
            target = build_job_target(item.target_models)
            job_spec = JobSpec.for_file(
                pipeline_id=spec.pipeline_id,
                job_name=f"{_parser_id(spec)}: {item.relative_path} [{target.label}]",
                target_label=target.label,
                target_key=target.key,
                handler_version=item.handler_version,
                file_type=spec.file_type,
                file_id=item.file_id,
                hash_id=item.hash_id,
            )

            _stage_archive(item.source_path, stage_dir, [item.relative_path])

            def _runner(report_progress):
                if not full_path.exists():
                    raise FileNotFoundError(f"Staged file missing: {full_path}")
                return process_file(
                    eng=eng,
                    spec=spec,
                    hash_id=item.hash_id,
                    file_path=full_path,
                    report_progress=report_progress,
                    target_models=list(item.target_models),
                    db_workers=args.db_workers,
                    force=args.force,
                )

            result = run_processing_job(eng, session_id, job_spec, _runner)
            total_rows += result.rows_uploaded
        except Exception as exc:
            session_failed = True
            failed += 1
            copied = _copy_failure_artifact(full_path, failure_dir, item)
            job_id = (
                _lookup_processing_job_id(eng, session_id, job_spec)
                if session_id is not None and job_spec is not None
                else None
            )
            print(
                f"FAILED archive={item.archive_id} file={item.file_id} "
                f"hash={item.hash_id} job={job_id or 'n/a'} "
                f"parser={_parser_id(spec)} relative_path={item.relative_path} "
                f"error={exc}"
            )
            if copied is not None:
                print(f"Failure file copied to {copied}")
        finally:
            if session_id is not None:
                finalize_processing_session(eng, session_id, failed=session_failed)
            if stage_dir.exists() and not args.keep_staging:
                shutil.rmtree(stage_dir, ignore_errors=True)

    print(
        json.dumps(
            {
                "parser": _parser_id(spec),
                "processed": len(items),
                "failed": failed,
                "rows_uploaded": total_rows,
            },
            indent=2,
        )
    )
    return 1 if failed else 0


def _resolve_work_parser(args):
    specs = _handled_specs()
    if getattr(args, "from_file", None):
        spec, _source = _resolve_parser_from_file(args.from_file, None, specs)
        return spec
    if not args.parser:
        raise SystemExit("Provide a parser or --from-file.")
    return _resolve_parser(args.parser, specs)


def _run_parser_work_command(args, *, failed_only: bool = False) -> int:
    _apply_target_env(args, require_db=True)
    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine

    spec = _resolve_work_parser(args)
    _print_target(_target_info(f"{args.command} {_parser_id(spec)}"))
    eng = get_engine(settings.database)
    try:
        items, file_rows, hashes = _plan_parser_work(
            eng,
            spec,
            limit=args.limit,
            failed_only=failed_only,
            force=args.force,
        )
        _print_parser_plan(spec, items, file_rows, hashes)
        if args.plan or args.dry_run:
            return 0
        if not items:
            if file_rows == 0:
                print(
                    "No current catalog rows found for this parser. "
                    "Run refresh/scan first if this file pattern is new."
                )
            return 0
        return _run_parser_items(args, eng, spec, items)
    finally:
        eng.dispose()


def cmd_refresh(args) -> int:
    _apply_target_env(args, require_db=True)
    if args.staging_root:
        os.environ["E2UDE_PATHS__STAGING_ROOT"] = str(
            args.staging_root.expanduser().resolve()
        )
    _print_target(_target_info("refresh"))
    if args.preview or args.dry_run:
        return 0

    from e2ude_core.config import settings
    from e2ude_core.db.schema_safety import (
        is_protected_schema,
        require_exact_confirmation,
    )

    if (
        settings.database.type == "mssql"
        and is_protected_schema(settings.database.schema_name)
        and settings.database.schema_name == ENV_SCHEMAS["prod"]
    ):
        require_exact_confirmation(
            expected_schema=settings.database.schema_name,
            provided_schema=args.confirm,
            flag_name="--confirm",
        )

    from e2ude_core.main import main as refresh_main

    refresh_main()
    return 0


def cmd_trial(args) -> int:
    args.command = "trial"
    if args.limit is None:
        args.limit = 50
    return _run_parser_work_command(args)


def cmd_backfill(args) -> int:
    args.command = "backfill"
    return _run_parser_work_command(args)


def cmd_retry_failed(args) -> int:
    args.command = "retry-failed"
    return _run_parser_work_command(args, failed_only=True)


def cmd_artifacts_status(args) -> int:
    return cmd_parser_status(args)


def cmd_artifacts_invalidate(args) -> int:
    _apply_target_env(args, require_db=True)
    import sqlalchemy as sa

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.models import ArtifactManifest

    spec = _resolve_parser(args.parser, _handled_specs())
    target_tables = [model.__tablename__ for model in spec.expected_models]
    _print_target(_target_info(f"artifacts invalidate {_parser_id(spec)}"))

    eng = get_engine(settings.database)
    try:
        with eng.begin() as conn:
            predicate = ArtifactManifest.target_table.in_(target_tables)
            if args.hash_id:
                predicate = predicate & ArtifactManifest.hash_id.in_(args.hash_id)
            count = conn.execute(
                sa.select(sa.func.count())
                .select_from(ArtifactManifest)
                .where(predicate)
            ).scalar_one()
            print(f"Parser      {_parser_id(spec)}")
            print(f"Tables      {', '.join(target_tables)}")
            print(f"Artifacts   {count}")
            if args.dry_run or args.plan:
                return 0
            if not args.yes:
                raise SystemExit("Refusing invalidate without --yes.")
            conn.execute(ArtifactManifest.__table__.delete().where(predicate))
            print(f"Deleted {count} artifact manifest rows.")
    finally:
        eng.dispose()
    return 0


def _schema_exists(conn, schema_name: str) -> bool:
    import sqlalchemy as sa

    return (
        conn.execute(
            sa.text("SELECT 1 FROM sys.schemas WHERE name = :schema_name"),
            {"schema_name": schema_name},
        ).scalar_one_or_none()
        is not None
    )


def _fetch_schema_tables(conn, schema_name: str) -> list[str]:
    import sqlalchemy as sa

    rows = conn.execute(
        sa.text(
            """
            SELECT t.name
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name
            ORDER BY t.name
            """
        ),
        {"schema_name": schema_name},
    ).fetchall()
    return [row.name for row in rows]


def _drop_schema_tables(conn, schema_name: str, table_names: list[str]) -> None:
    import sqlalchemy as sa

    fk_rows = conn.execute(
        sa.text(
            """
            SELECT OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
                   OBJECT_NAME(fk.parent_object_id) AS table_name,
                   fk.name AS constraint_name
            FROM sys.foreign_keys AS fk
            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name
            """
        ),
        {"schema_name": schema_name},
    ).fetchall()
    for row in fk_rows:
        conn.execute(
            sa.text(
                f"ALTER TABLE [{row.schema_name}].[{row.table_name}] "
                f"DROP CONSTRAINT [{row.constraint_name}]"
            )
        )
    for table_name in table_names:
        conn.execute(sa.text(f"DROP TABLE [{schema_name}].[{table_name}]"))


def _source_columns(conn, schema_name: str, table_name: str) -> set[str]:
    import sqlalchemy as sa

    rows = conn.execute(
        sa.text(
            """
            SELECT c.name
            FROM sys.columns AS c
            INNER JOIN sys.tables AS t ON t.object_id = c.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name AND t.name = :table_name
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    ).fetchall()
    return {row.name for row in rows}


def _copyable_columns(table, source_columns: set[str]) -> tuple[list[str], list[str]]:
    common = [column.name for column in table.columns if column.name in source_columns]
    missing_required = []
    for column in table.columns:
        if column.name in source_columns:
            continue
        has_default = column.default is not None or column.server_default is not None
        if not column.nullable and not has_default and not column.autoincrement:
            missing_required.append(column.name)
    return common, missing_required


def _uses_identity_insert(table, column_names: list[str]) -> bool:
    if "id" not in column_names:
        return False
    id_column = table.columns.get("id")
    return bool(id_column is not None and id_column.primary_key)


def _copy_table_rows(conn, source_schema: str, dest_schema: str, table) -> int | None:
    import sqlalchemy as sa

    table_name = table.name
    source_table_names = _fetch_schema_tables(conn, source_schema)
    if table_name not in source_table_names:
        return None

    common, missing_required = _copyable_columns(
        table, _source_columns(conn, source_schema, table_name)
    )
    if missing_required:
        print(
            f"Skipped [{table_name}]; source missing required columns: "
            f"{', '.join(missing_required)}"
        )
        return None
    if not common:
        print(f"Skipped [{table_name}]; no compatible columns found.")
        return None

    columns = ", ".join(f"[{name}]" for name in common)
    identity_insert = _uses_identity_insert(table, common)
    if identity_insert:
        conn.execute(sa.text(f"SET IDENTITY_INSERT [{dest_schema}].[{table_name}] ON"))
    try:
        result = conn.execute(
            sa.text(
                f"INSERT INTO [{dest_schema}].[{table_name}] ({columns}) "
                f"SELECT {columns} FROM [{source_schema}].[{table_name}]"
            )
        )
    finally:
        if identity_insert:
            conn.execute(
                sa.text(f"SET IDENTITY_INSERT [{dest_schema}].[{table_name}] OFF")
            )
    return result.rowcount


def cmd_schema_cleanup(args) -> int:
    raw_schema = getattr(args, "schema_name", None) or getattr(args, "schema", None)
    if not raw_schema:
        raise SystemExit("Provide a schema name to clean up.")
    schema_name = _resolve_schema_ref(raw_schema)
    _apply_schema_command_env(args, schema_name)
    import sqlalchemy as sa

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.schema_safety import require_exact_confirmation

    if settings.database.type != "mssql":
        raise SystemExit("schema cleanup only supports MSSQL targets.")

    _print_target(_target_info(f"schema cleanup {schema_name}"))
    eng = get_engine(settings.database)
    try:
        with eng.begin() as conn:
            if not _schema_exists(conn, schema_name):
                print(f"Schema [{schema_name}] does not exist.")
                return 0
            table_names = _fetch_schema_tables(conn, schema_name)
            for table_name in table_names:
                print(f"  - {table_name}")
            if args.preview or args.dry_run:
                return 0
            if not args.yes:
                raise SystemExit("Refusing cleanup without --yes.")
            require_exact_confirmation(
                expected_schema=schema_name,
                provided_schema=args.confirm_schema,
                flag_name="--confirm-schema",
            )
            _drop_schema_tables(conn, schema_name, table_names)
            if not args.keep_schema:
                conn.execute(sa.text(f"DROP SCHEMA [{schema_name}]"))
            print(f"Dropped {len(table_names)} tables from [{schema_name}].")
    finally:
        eng.dispose()
    return 0


def cmd_schema_check(args) -> int:
    schema_name = _resolve_schema_ref(args.schema)
    _apply_schema_command_env(args, schema_name)
    import sqlalchemy as sa

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.setup import _missing_runtime_columns, _runtime_tables

    if settings.database.type != "mssql":
        raise SystemExit("schema check only supports MSSQL targets.")

    _print_target(_target_info(f"schema check {schema_name}"))
    eng = get_engine(settings.database)
    try:
        tables = _runtime_tables()
        missing = _missing_runtime_columns(eng, tables)
        with eng.connect() as conn:
            if not _schema_exists(conn, schema_name):
                print(f"Schema [{schema_name}] does not exist.")
                return 1
            table_names = _fetch_schema_tables(conn, schema_name)
            print(f"Tables      {len(table_names)}")
            if missing:
                print("Missing runtime columns")
                for item in sorted(missing):
                    print(f"  - {item}")
                return 1
            print("Runtime schema OK")

            if "processing_jobs" in table_names:
                running_jobs = conn.execute(
                    sa.text(
                        f"SELECT COUNT(*) FROM [{schema_name}].[processing_jobs] "
                        "WHERE status = 'RUNNING'"
                    )
                ).scalar_one()
                error_jobs = conn.execute(
                    sa.text(
                        f"SELECT COUNT(*) FROM [{schema_name}].[processing_jobs] "
                        "WHERE status = 'ERROR'"
                    )
                ).scalar_one()
                print(f"Running jobs {running_jobs}")
                print(f"Error jobs   {error_jobs}")
                if running_jobs:
                    return 1

            counts = _parser_counts(eng, _handled_specs())
            rows = [
                [
                    parser,
                    values["hashes"],
                    values["complete"],
                    values["missing"],
                    values["rows"],
                ]
                for parser, values in sorted(counts.items())
            ]
            if rows:
                _print_table(
                    ["parser", "hashes", "complete", "missing/stale", "rows"],
                    rows,
                )
    finally:
        eng.dispose()
    return 0


def cmd_schema_clone(args) -> int:
    source_schema = _resolve_schema_ref(args.source)
    dest_schema = _resolve_schema_ref(args.dest)
    _apply_schema_command_env(args, dest_schema)

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.schema_safety import is_protected_schema
    from e2ude_core.db.setup import initialize_database, _runtime_tables

    if settings.database.type != "mssql":
        raise SystemExit("schema clone only supports MSSQL targets.")
    if source_schema == dest_schema:
        raise SystemExit("source and destination schemas must be different.")
    if is_protected_schema(dest_schema):
        raise SystemExit(f"Protected schema [{dest_schema}] cannot be cloned into.")

    _print_target(_target_info(f"schema clone {source_schema} -> {dest_schema}"))
    eng = get_engine(settings.database)
    try:
        with eng.begin() as conn:
            if not _schema_exists(conn, source_schema):
                raise SystemExit(f"Source schema [{source_schema}] does not exist.")
            if _schema_exists(conn, dest_schema):
                table_names = _fetch_schema_tables(conn, dest_schema)
                if table_names and not args.replace:
                    raise SystemExit(
                        f"Destination schema [{dest_schema}] is not empty. "
                        "Use --replace to rebuild it."
                    )
                if args.replace and table_names:
                    if args.dry_run or args.preview:
                        print(f"Would drop {len(table_names)} destination tables.")
                        return 0
                    if not args.yes:
                        raise SystemExit("Refusing clone --replace without --yes.")
                    _drop_schema_tables(conn, dest_schema, table_names)
            if args.dry_run or args.preview:
                print(f"Would clone compatible runtime tables into [{dest_schema}].")
                return 0

        initialize_database(eng, reset_tables=False)

        copied: list[list[object]] = []
        skipped: list[str] = []
        with eng.begin() as conn:
            for table in _runtime_tables():
                if table.name in AUDIT_TABLES:
                    skipped.append(table.name)
                    continue
                rowcount = _copy_table_rows(conn, source_schema, dest_schema, table)
                if rowcount is None:
                    skipped.append(table.name)
                else:
                    copied.append([table.name, rowcount])

        if copied:
            _print_table(["table", "rows"], copied)
        if skipped:
            print("Skipped")
            for table_name in skipped:
                print(f"  - {table_name}")
    finally:
        eng.dispose()
    return 0


def cmd_schema_promote(args) -> int:
    raw_source = getattr(args, "source_option", None) or args.source
    raw_dest = getattr(args, "dest_option", None) or args.dest
    if not raw_source:
        raise SystemExit("Provide a source schema to promote.")
    source_schema = _resolve_schema_ref(raw_source)
    target_schema = _resolve_schema_ref(raw_dest)
    _apply_schema_command_env(args, target_schema)
    import sqlalchemy as sa

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.schema_safety import (
        is_protected_schema,
        require_exact_confirmation,
    )

    if settings.database.type != "mssql":
        raise SystemExit("schema promote only supports MSSQL targets.")

    archive_schema = validate_schema_name(
        args.archive
        or f"{target_schema}_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    )
    if source_schema == target_schema:
        raise SystemExit("--source and --dest must be different schemas.")
    if archive_schema in {source_schema, target_schema}:
        raise SystemExit("--archive must be distinct from --source and --dest.")
    if is_protected_schema(source_schema):
        raise SystemExit(f"Protected schema [{source_schema}] cannot be promoted from.")
    if is_protected_schema(archive_schema):
        raise SystemExit(f"Protected schema [{archive_schema}] cannot be an archive.")
    _print_target(_target_info(f"schema promote {source_schema} -> {target_schema}"))

    eng = get_engine(settings.database)
    try:
        with eng.begin() as conn:
            if not _schema_exists(conn, source_schema):
                raise SystemExit(f"Source schema [{source_schema}] does not exist.")
            source_tables = _fetch_schema_tables(conn, source_schema)
            target_tables = (
                _fetch_schema_tables(conn, target_schema)
                if _schema_exists(conn, target_schema)
                else []
            )
            archive_tables = (
                _fetch_schema_tables(conn, archive_schema)
                if _schema_exists(conn, archive_schema)
                else []
            )
            print(f"Source [{source_schema}]: {len(source_tables)} tables")
            print(f"Target [{target_schema}]: {len(target_tables)} tables")
            print(f"Archive [{archive_schema}]")
            if archive_tables:
                raise SystemExit(f"Archive schema [{archive_schema}] is not empty.")
            if args.preview or args.dry_run:
                return 0
            if not args.yes:
                raise SystemExit("Refusing promote without --yes.")
            if is_protected_schema(target_schema):
                require_exact_confirmation(
                    expected_schema=target_schema,
                    provided_schema=args.confirm or args.confirm_target_schema,
                    flag_name="--confirm",
                )
            if not _schema_exists(conn, target_schema):
                conn.execute(sa.text(f"CREATE SCHEMA [{target_schema}]"))
            if not _schema_exists(conn, archive_schema):
                conn.execute(sa.text(f"CREATE SCHEMA [{archive_schema}]"))
            for table_name in target_tables:
                conn.execute(
                    sa.text(
                        f"ALTER SCHEMA [{archive_schema}] "
                        f"TRANSFER [{target_schema}].[{table_name}]"
                    )
                )
            for table_name in source_tables:
                conn.execute(
                    sa.text(
                        f"ALTER SCHEMA [{target_schema}] "
                        f"TRANSFER [{source_schema}].[{table_name}]"
                    )
                )
            print(f"Promoted {len(source_tables)} tables.")
    finally:
        eng.dispose()
    return 0


def add_target_args(parser) -> None:
    parser.add_argument("--target", choices=sorted(MSSQL_TARGET_SCHEMAS))
    parser.add_argument("--env", choices=sorted(ENV_SCHEMAS))
    parser.add_argument("--schema", help="MSSQL schema override")
    parser.add_argument("--sqlite", type=Path, help="Use a local SQLite database")
    parser.add_argument("--config", type=Path, help="Path to e2ude_config.toml")


def add_mssql_connection_args(parser) -> None:
    parser.add_argument("--target", choices=sorted(MSSQL_TARGET_SCHEMAS))
    parser.add_argument("--config", type=Path, help="Path to e2ude_config.toml")


def add_work_args(parser) -> None:
    add_target_args(parser)
    parser.add_argument("parser", nargs="?", help="Parser id, file type, or prefix")
    parser.add_argument(
        "--from-file",
        type=Path,
        help="Infer the parser from a local path, then run matching cataloged work",
    )
    parser.add_argument("--limit", type=int, help="Maximum distinct hashes to process")
    parser.add_argument("--plan", action="store_true", help="Show work without writing")
    parser.add_argument("--dry-run", action="store_true", help="Alias for --plan")
    parser.add_argument(
        "--force", action="store_true", help="Rebuild current artifacts"
    )
    parser.add_argument("--db-workers", type=int, default=4)
    parser.add_argument("--staging-root", type=Path)
    parser.add_argument("--failure-dir", type=Path)
    parser.add_argument("--keep-staging", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="e2ude")
    sub = parser.add_subparsers(dest="command_name", required=True)

    refresh = sub.add_parser("refresh")
    add_target_args(refresh)
    refresh.add_argument("--preview", action="store_true")
    refresh.add_argument("--dry-run", action="store_true", help="Alias for --preview")
    refresh.add_argument("--confirm")
    refresh.add_argument("--staging-root", type=Path)
    refresh.set_defaults(func=cmd_refresh)

    parsers = sub.add_parser("parsers")
    add_target_args(parsers)
    parsers.add_argument("--counts", action="store_true")
    parsers.set_defaults(func=cmd_parsers)

    preview = sub.add_parser("preview")
    preview.add_argument("file_path", type=Path)
    preview.add_argument("--as", dest="as_parser")
    preview.add_argument("--head", type=int, default=5)
    preview.set_defaults(func=cmd_preview)

    trial = sub.add_parser("trial")
    add_work_args(trial)
    trial.set_defaults(func=cmd_trial)

    backfill = sub.add_parser("backfill")
    add_work_args(backfill)
    backfill.set_defaults(func=cmd_backfill)

    retry = sub.add_parser("retry-failed")
    add_work_args(retry)
    retry.set_defaults(func=cmd_retry_failed)

    parser_cmd = sub.add_parser("parser")
    parser_sub = parser_cmd.add_subparsers(dest="parser_command", required=True)

    parser_list = parser_sub.add_parser("list")
    add_target_args(parser_list)
    parser_list.add_argument("--counts", action="store_true")
    parser_list.set_defaults(func=cmd_parsers)

    parser_status = parser_sub.add_parser("status")
    add_target_args(parser_status)
    parser_status.add_argument("parser", nargs="?")
    parser_status.set_defaults(func=cmd_parser_status)

    parser_preview = parser_sub.add_parser("preview")
    parser_preview.add_argument("file_path", type=Path)
    parser_preview.add_argument("--as", dest="as_parser")
    parser_preview.add_argument("--head", type=int, default=5)
    parser_preview.set_defaults(func=cmd_preview)

    parser_trial = parser_sub.add_parser("trial")
    add_work_args(parser_trial)
    parser_trial.set_defaults(func=cmd_trial)

    parser_backfill = parser_sub.add_parser("backfill")
    add_work_args(parser_backfill)
    parser_backfill.set_defaults(func=cmd_backfill)

    parser_retry = parser_sub.add_parser("retry-failed")
    add_work_args(parser_retry)
    parser_retry.set_defaults(func=cmd_retry_failed)

    artifacts = sub.add_parser("artifacts")
    artifacts_sub = artifacts.add_subparsers(dest="artifacts_command", required=True)

    artifacts_status = artifacts_sub.add_parser("status")
    add_target_args(artifacts_status)
    artifacts_status.add_argument("parser", nargs="?")
    artifacts_status.set_defaults(func=cmd_artifacts_status)

    invalidate = artifacts_sub.add_parser("invalidate")
    add_target_args(invalidate)
    invalidate.add_argument("parser")
    invalidate.add_argument("--hash-id", action="append", type=int)
    invalidate.add_argument("--plan", action="store_true")
    invalidate.add_argument("--dry-run", action="store_true")
    invalidate.add_argument("--yes", action="store_true")
    invalidate.set_defaults(func=cmd_artifacts_invalidate)

    schema = sub.add_parser("schema")
    schema_sub = schema.add_subparsers(dest="schema_command", required=True)

    cleanup = schema_sub.add_parser("cleanup")
    add_mssql_connection_args(cleanup)
    cleanup.add_argument("schema_name", nargs="?")
    cleanup.add_argument("--schema")
    cleanup.add_argument("--preview", action="store_true")
    cleanup.add_argument("--dry-run", action="store_true")
    cleanup.add_argument("--yes", action="store_true")
    cleanup.add_argument("--confirm-schema")
    cleanup.add_argument("--keep-schema", action="store_true")
    cleanup.set_defaults(func=cmd_schema_cleanup)

    clone = schema_sub.add_parser("clone")
    add_mssql_connection_args(clone)
    clone.add_argument("source")
    clone.add_argument("dest")
    clone.add_argument("--replace", action="store_true")
    clone.add_argument("--preview", action="store_true")
    clone.add_argument("--dry-run", action="store_true")
    clone.add_argument("--yes", action="store_true")
    clone.set_defaults(func=cmd_schema_clone)

    check = schema_sub.add_parser("check")
    add_mssql_connection_args(check)
    check.add_argument("schema")
    check.set_defaults(func=cmd_schema_check)

    promote = schema_sub.add_parser("promote")
    add_mssql_connection_args(promote)
    promote.add_argument("source", nargs="?")
    promote.add_argument("dest", nargs="?", default="prod")
    promote.add_argument("--source", dest="source_option")
    promote.add_argument("--dest", dest="dest_option")
    promote.add_argument("--archive")
    promote.add_argument("--preview", action="store_true")
    promote.add_argument("--dry-run", action="store_true")
    promote.add_argument("--yes", action="store_true")
    promote.add_argument("--confirm")
    promote.add_argument("--confirm-target-schema")
    promote.set_defaults(func=cmd_schema_promote)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
