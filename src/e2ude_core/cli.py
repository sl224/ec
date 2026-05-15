from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Sequence

import pandas as pd

from e2ude_core.db.schema_safety import schema_classification, validate_schema_name

HELP_FORMATTER = argparse.RawDescriptionHelpFormatter

TOP_LEVEL_HELP = """
Common workflows:
  Refresh shared dev:
    e2ude refresh --env dev

  List parsers:
    e2ude parser list

  Check parser backlog:
    e2ude parser status --env dev

  Preview one local parser input:
    e2ude parser preview C:\\tmp\\sample_Engine --as engine_on_off

  Backfill one parser across cataloged history:
    e2ude parser backfill engine_on_off --env dev --plan
    e2ude parser backfill engine_on_off --env dev

  Rebuild one parser's current outputs:
    e2ude parser backfill engine_on_off --env dev --force --plan
    e2ude parser backfill engine_on_off --env dev --force

Targets:
  --env dev      shared dev SQL Server schema: e2ude_core_dev
  --env prod     shared prod SQL Server schema: e2ude_core
  --schema NAME  custom SQL Server schema for candidates or experiments
  --sqlite PATH  local SQLite database
""".strip()

REFRESH_HELP = """
Normal incremental ingest.

Use this when new archives may have arrived. It discovers archives, catalogs ZIP
contents, hashes only files needed by active parsers, and parses missing or stale
outputs. Unchanged archive locators are skipped. If an archive moves, the new
locator is cataloged, but parser outputs still dedupe by file content hash.
""".strip()

PARSER_HELP = """
Parser development and parser-output operations.

Use this group when you are writing a parser, checking parser coverage, running
one parser across history, or marking one parser's outputs stale.
""".strip()

PARSER_LIST_HELP = """
List parser ids, accepted file-name hints, ZIP-member patterns, and output tables.

Use this when you do not remember whether a parser is called engine_on_off,
tmptr_log, segments, mcdata, or something else. Add --counts with a target to
include cataloged files, hashes, complete outputs, stale work, and row counts.
""".strip()

PARSER_STATUS_HELP = """
Show parser coverage and remaining work.

Columns:
  files          cataloged ZIP members matching this parser
  hashed         matching files with content hashes available
  hashes         distinct content hashes
  complete       hashes with current parser artifacts
  missing/stale  hashes needing parse or rebuild
  rows           rows recorded in artifact manifest
""".strip()

PARSER_PREVIEW_HELP = """
Parse one local file and print a JSON preview.

Use this while developing parser logic. It does not read or write the database,
does not require refresh, and does not require the file to come from the RSM
network drive. If the filename does not match a known pattern, pass --as PARSER.
""".strip()

PARSER_BACKFILL_HELP = """
Run one parser against matching cataloged archive files.

Use this when:
  - you added a new parser and want to parse all historical matching files
  - you changed parser logic and want to rebuild that parser's outputs
  - you want to process one parser without touching unrelated parsers

Backfill does not discover new archives. Run e2ude refresh first if the
archive/file catalog is out of date.

Examples:
  e2ude parser backfill engine_on_off --env dev --plan
      Show how much historical engine_on_off work exists.

  e2ude parser backfill engine_on_off --env dev
      Parse all missing/stale engine_on_off outputs.

  e2ude parser backfill --from-file C:\\tmp\\sample_Engine --env dev --plan
      Infer the parser from a local example file, then show matching cataloged
      work. This does not parse that local file and does not add it to the
      database.

  e2ude parser backfill engine_on_off --env dev --force
      Rebuild current engine_on_off outputs even if they are marked complete.
""".strip()

PARSER_INVALIDATE_HELP = """
Delete manifest rows for one parser.

Use this when old parsed results should be discarded and rebuilt on the next
backfill. This deletes that parser's current rows and manifest rows for the
matching content hashes.
""".strip()

SCHEMA_SEED_HELP = """
Warm a fresh MSSQL schema from stable content-addressed facts.

The destination schema is initialized from current code. Current archive
locators come from a fresh scan. Source catalog rows are reused by locator or
catalog signature. Source parser rows are reused only when the current runtime
spec, parser version, artifact key, and table shape still match.

Seed does not copy source archive inventory rows or processing audit rows. Run
refresh afterward to finish work that could not be reused.
""".strip()

SCHEMA_HELP = """
MSSQL schema operations for candidate validation and promotion.

Use schema commands when you want to build in a disposable schema, validate it,
then promote the schema as a unit.

Use schema seed when a fresh candidate can reuse old content-addressed catalog
and parser rows before refresh finishes the remaining work.
""".strip()

TARGET_HELP = {
    "env": "Shared MSSQL environment: dev -> e2ude_core_dev, prod -> e2ude_core.",
    "schema": "Custom MSSQL schema for candidates or experiments.",
    "sqlite": "Local SQLite database file.",
    "config": "Path to e2ude_config.toml.",
}

ENV_SCHEMAS = {
    "dev": "e2ude_core_dev",
    "prod": "e2ude_core",
}


if TYPE_CHECKING:
    from e2ude_core.orchestration.state import ParserWorkItem


@dataclass(frozen=True)
class TargetInfo:
    backend: str
    command: str
    server: str | None = None
    database: str | None = None
    schema: str | None = None
    schema_class: str | None = None
    sqlite_path: str | None = None


def _parser_id(spec) -> str:
    return spec.parser_id


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

    env_name = getattr(args, "env", None)
    sqlite_path = getattr(args, "sqlite", None)
    schema_name = getattr(args, "schema", None)

    if sum(bool(value) for value in (env_name, sqlite_path)) > 1:
        raise SystemExit("Choose only one of --env or --sqlite.")
    if sqlite_path and schema_name:
        raise SystemExit("--schema applies only to MSSQL targets.")
    if env_name and schema_name:
        raise SystemExit(
            "Choose --env for a shared schema or --schema for a custom schema."
        )
    if require_db and not env_name and not sqlite_path and not schema_name:
        raise SystemExit("Choose --env dev, --env prod, --schema NAME, or --sqlite.")

    if sqlite_path:
        db_path = Path(sqlite_path).expanduser().resolve()
        os.environ["E2UDE_DATABASE__TYPE"] = "sqlite3"
        os.environ["E2UDE_DATABASE__DB_LOCATION"] = str(db_path)
        os.environ["E2UDE_DATABASE__IN_MEMORY"] = "false"
        os.environ.pop("E2UDE_DATABASE__SCHEMA_NAME", None)
        return

    if env_name or schema_name:
        if env_name:
            schema = ENV_SCHEMAS[env_name]
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

    from e2ude_core.runtime_files import detect_file_type

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


def _empty_parser_counts(specs) -> dict[str, dict[str, int]]:
    return {
        _parser_id(spec): {
            "files": 0,
            "hashed": 0,
            "hashes": 0,
            "complete": 0,
            "missing": 0,
            "rows": 0,
        }
        for spec in specs
    }


def _parse_content_hash(value: str) -> bytes:
    try:
        content_hash = bytes.fromhex(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("content hash must be hex") from exc
    if len(content_hash) != 16:
        raise argparse.ArgumentTypeError("content hash must be 32 hex characters")
    return content_hash


def _catalog_tables_exist(eng) -> bool:
    import sqlalchemy as sa

    from e2ude_core.db.base_session import DEFAULT_SCHEMA

    inspector = sa.inspect(eng)
    return inspector.has_table(
        "metadata_archive", schema=DEFAULT_SCHEMA
    ) and inspector.has_table("metadata_file", schema=DEFAULT_SCHEMA)


def _parser_counts(eng, specs) -> dict[str, dict[str, int]]:
    from e2ude_core.orchestration.state import count_parser_artifacts

    if not _catalog_tables_exist(eng):
        return _empty_parser_counts(specs)
    return count_parser_artifacts(eng, specs)


def cmd_parsers(args) -> int:
    if args.counts:
        _apply_target_env(args, require_db=True)
        from e2ude_core.config import settings
        from e2ude_core.db.access import get_engine

        specs = _handled_specs()
        _print_target(_target_info("parser list --counts"))
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
        headers.extend(
            ["files", "hashed", "hashes", "complete", "missing/stale", "rows"]
        )

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
                    parser_counts["hashed"],
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
                parser_counts["hashed"],
                parser_counts["hashes"],
                parser_counts["complete"],
                parser_counts["missing"],
                parser_counts["rows"],
            ]
        )

    _print_table(
        [
            "parser",
            "version",
            "files",
            "hashed",
            "hashes",
            "complete",
            "missing/stale",
            "rows",
        ],
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
                "parser_id": _parser_id(spec),
                "selection_source": selection_source,
                "parser_version": spec.version,
                "tables": output_tables,
            },
            indent=2,
            default=str,
        )
    )
    return 0


def _plan_parser_work(
    eng,
    spec,
    *,
    limit: int | None = None,
    force: bool = False,
):
    from e2ude_core.orchestration.state import (
        count_parser_artifacts,
        group_pending_artifacts,
        load_pending_artifacts,
    )

    artifacts = load_pending_artifacts(
        eng,
        parser_id=_parser_id(spec),
        limit=limit,
        force=force,
    )
    items = list(group_pending_artifacts(artifacts))
    counts = count_parser_artifacts(eng, (spec,))[_parser_id(spec)]
    return (
        items,
        counts["files"],
        counts.get("hashed", counts["hashes"]),
        counts["hashes"],
    )


def _print_parser_plan(
    spec, items: list[ParserWorkItem], file_rows: int, hashed: int, hashes: int
) -> None:
    print(f"Parser      {_parser_id(spec)}")
    print(f"File type   {spec.file_type.value}")
    print(f"Version     {spec.version}")
    print(f"Files       {file_rows}")
    print(f"Hashed      {hashed}")
    print(f"Hashes      {hashes}")
    print(f"Pending     {len(items)}")
    if not items:
        print("No pending parser artifacts found.")
        return

    rows = [
        [
            item.archive_id,
            item.file_id,
            item.content_hash.hex() if item.content_hash is not None else "-",
            item.relative_path,
            ", ".join(model.__tablename__ for model in item.target_models),
        ]
        for item in items[:20]
    ]
    _print_table(
        ["archive", "file", "content_hash", "relative_path", "missing_outputs"], rows
    )
    if len(items) > 20:
        print(f"... {len(items) - 20} more")


def _copy_failure_artifact(
    path: Path, failure_dir: Path, item: ParserWorkItem
) -> Path | None:
    if not path.exists():
        return None
    failure_dir.mkdir(parents=True, exist_ok=True)
    hash_text = item.content_hash.hex() if item.content_hash is not None else "unhashed"
    target = failure_dir / (
        f"archive_{item.archive_id}_file_{item.file_id}_hash_{hash_text}_{path.name}"
    )
    shutil.copy2(path, target)
    return target


def _stage_archive(
    zip_path: Path, local_dir: Path, relative_paths: Sequence[str]
) -> None:
    from e2ude_core.services.zip_io import extract_archive_members

    if local_dir.exists():
        shutil.rmtree(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    extract_archive_members(zip_path, local_dir, relative_paths)


def _run_parser_items(args, eng, spec, items: list[ParserWorkItem]) -> int:
    from e2ude_core.config import settings
    from e2ude_core.context import EtlContext
    from e2ude_core.orchestration.catalog import HASH_PIPELINE_ID, hash_catalog_file
    from e2ude_core.orchestration.runs import (
        create_processing_job,
        create_processing_session,
        finalize_processing_session,
        mark_processing_job_completed,
        mark_processing_job_failed,
        mark_processing_job_running,
        set_processing_job_content_hash,
    )
    from e2ude_core.orchestration.state import target_models_needing_work
    from e2ude_core.pipelines.base import process_file

    staging_root = Path(args.staging_root or settings.paths.staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)
    failure_dir = Path(args.failure_dir or staging_root / "failures")
    context = EtlContext.capture()

    total_rows = 0
    failed = 0
    session_failed = False
    session_id = create_processing_session(eng, context)
    for item in items:
        job_id = None
        stage_dir = staging_root / (
            f"cli_{_parser_id(spec)}_{item.archive_id}_{item.file_id}"
        )
        full_path = stage_dir / item.relative_path
        try:
            content_hash = item.content_hash
            if content_hash is None:
                job_id = create_processing_job(
                    eng,
                    session_id,
                    archive_id=item.archive_id,
                    file_id=item.file_id,
                    content_hash=None,
                    file_type=spec.file_type,
                    parser_id=HASH_PIPELINE_ID,
                    target_table="metadata_file",
                    parser_version=1,
                )
                mark_processing_job_running(eng, job_id, "Hashing catalog member")
                _stage_archive(item.locator_path, stage_dir, [item.relative_path])
                if not full_path.exists():
                    raise FileNotFoundError(f"Staged file missing: {full_path}")
                content_hash = hash_catalog_file(eng, item.file_id, full_path)
                set_processing_job_content_hash(eng, job_id, content_hash)
                mark_processing_job_completed(
                    eng,
                    job_id,
                    message="Hash recorded",
                    rows_uploaded=1,
                )
                job_id = None

            target_models = target_models_needing_work(
                eng,
                content_hash=content_hash,
                spec=spec,
                target_models=item.target_models,
                force=args.force,
            )
            if not target_models:
                continue

            job_id = create_processing_job(
                eng,
                session_id,
                archive_id=item.archive_id,
                file_id=item.file_id,
                content_hash=content_hash,
                file_type=spec.file_type,
                parser_id=_parser_id(spec),
                target_table=None,
                parser_version=item.parser_version,
            )
            if not full_path.exists():
                _stage_archive(item.locator_path, stage_dir, [item.relative_path])
                if not full_path.exists():
                    raise FileNotFoundError(f"Staged file missing: {full_path}")

            def _progress(message: str) -> None:
                mark_processing_job_running(eng, job_id, message)

            _progress(f"Starting {_parser_id(spec)}")
            result = process_file(
                eng=eng,
                spec=spec,
                content_hash=content_hash,
                file_path=full_path,
                report_progress=_progress,
                target_models=target_models,
                force=args.force,
            )
            total_rows += result.rows_uploaded
            mark_processing_job_completed(
                eng,
                job_id,
                message=result.completion_message or "Completed",
                rows_uploaded=result.rows_uploaded,
            )
        except Exception as exc:
            session_failed = True
            failed += 1
            copied = _copy_failure_artifact(full_path, failure_dir, item)
            if job_id is not None:
                mark_processing_job_failed(eng, job_id, f"Failed: {exc}")
            print(
                f"FAILED archive={item.archive_id} file={item.file_id} "
                f"hash={content_hash.hex() if content_hash is not None else '-'} "
                f"job={job_id or 'n/a'} "
                f"parser={_parser_id(spec)} relative_path={item.relative_path} "
                f"error={exc}"
            )
            if copied is not None:
                print(f"Failure file copied to {copied}")
        finally:
            if stage_dir.exists() and not args.keep_staging:
                shutil.rmtree(stage_dir, ignore_errors=True)

    finalize_processing_session(eng, session_id, failed=session_failed)

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


def _run_parser_work_command(args) -> int:
    _apply_target_env(args, require_db=True)
    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine

    spec = _resolve_work_parser(args)
    _print_target(_target_info(f"{args.command} {_parser_id(spec)}"))
    eng = get_engine(settings.database)
    try:
        if not _catalog_tables_exist(eng):
            _print_parser_plan(spec, [], 0, 0, 0)
            print(
                "No e2ude metadata tables found in this target. "
                "Run refresh first, or check --schema/--sqlite."
            )
            return 0
        items, file_rows, hashed, hashes = _plan_parser_work(
            eng,
            spec,
            limit=args.limit,
            force=args.force,
        )
        _print_parser_plan(spec, items, file_rows, hashed, hashes)
        if args.plan:
            return 0
        if not items:
            if file_rows == 0:
                print(
                    "No current catalog rows found for this parser. "
                    "Run refresh first if this file pattern is new."
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
    if args.preview:
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


def cmd_parser_backfill(args) -> int:
    args.command = "backfill"
    return _run_parser_work_command(args)


def cmd_parser_invalidate(args) -> int:
    _apply_target_env(args, require_db=True)
    import sqlalchemy as sa

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.models import ArtifactManifest
    from e2ude_core.runtime_files import artifact_key_for

    spec = _resolve_parser(args.parser, _handled_specs())
    artifact_keys = [artifact_key_for(spec, model) for model in spec.expected_models]
    target_tables = [model.__tablename__ for model in spec.expected_models]
    _print_target(_target_info(f"parser invalidate {_parser_id(spec)}"))

    eng = get_engine(settings.database)
    try:
        with eng.begin() as conn:
            predicate = ArtifactManifest.artifact_key.in_(artifact_keys)
            if args.content_hash:
                predicate = predicate & ArtifactManifest.content_hash.in_(
                    args.content_hash
                )
            manifest_count = conn.execute(
                sa.select(sa.func.count())
                .select_from(ArtifactManifest)
                .where(predicate)
            ).scalar_one()
            table_counts = []
            for model in spec.expected_models:
                stmt = sa.select(sa.func.count()).select_from(model)
                if args.content_hash:
                    stmt = stmt.where(model.content_hash.in_(args.content_hash))
                table_counts.append(
                    [model.__tablename__, conn.execute(stmt).scalar_one()]
                )

            print(f"Parser      {_parser_id(spec)}")
            print(f"Artifact keys  {', '.join(artifact_keys)}")
            print(f"Tables         {', '.join(target_tables)}")
            print(f"Manifest rows  {manifest_count}")
            _print_table(["table", "rows"], table_counts)
            if args.plan:
                return 0
            if not args.yes:
                raise SystemExit("Refusing invalidate without --yes.")
            deleted_rows = 0
            for model in spec.expected_models:
                delete_stmt = model.__table__.delete()
                if args.content_hash:
                    delete_stmt = delete_stmt.where(
                        model.content_hash.in_(args.content_hash)
                    )
                result = conn.execute(delete_stmt)
                deleted_rows += result.rowcount or 0
            conn.execute(ArtifactManifest.__table__.delete().where(predicate))
            print(
                f"Deleted {deleted_rows} parser rows and "
                f"{manifest_count} artifact manifest rows."
            )
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


def _quote_mssql_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def _mssql_name(schema_name: str, table_name: str) -> str:
    return (
        f"{_quote_mssql_identifier(schema_name)}."
        f"{_quote_mssql_identifier(table_name)}"
    )


def _mssql_column_list(column_names: Sequence[str]) -> str:
    return ", ".join(_quote_mssql_identifier(name) for name in column_names)


def _rowcount(result) -> int:
    rowcount = result.rowcount
    return rowcount if rowcount is not None and rowcount >= 0 else 0


def _mssql_columns(conn, schema_name: str, table_name: str) -> set[str]:
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


def _mssql_has_table(conn, schema_name: str, table_name: str) -> bool:
    import sqlalchemy as sa

    return (
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM sys.tables AS t
                INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                WHERE s.name = :schema_name AND t.name = :table_name
                """
            ),
            {"schema_name": schema_name, "table_name": table_name},
        ).scalar_one_or_none()
        is not None
    )


def _locator_key_for_path(path: Path | str) -> str:
    return str(Path(path)).casefold()


def _source_archive_columns(conn, source_schema: str) -> tuple[str, str] | None:
    columns = _mssql_columns(conn, source_schema, "metadata_archive")
    if {
        "catalog_signature",
        "catalog_version",
        "locator_key",
        "locator_size_bytes",
    }.issubset(columns):
        return "locator_key", "locator_size_bytes"
    return None


def _source_catalog_archives(
    conn,
    source_schema: str,
    *,
    catalog_version: int,
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    import sqlalchemy as sa

    archive_columns = _source_archive_columns(conn, source_schema)
    if archive_columns is None:
        return {}, {}
    locator_column, size_column = archive_columns
    rows = conn.execute(
        sa.text(
            f"""
            SELECT id, {locator_column} AS locator_key,
                   {size_column} AS locator_size_bytes,
                   catalog_signature
            FROM {_mssql_name(source_schema, "metadata_archive")}
            WHERE cataloged_at IS NOT NULL
              AND catalog_version >= :catalog_version
              AND catalog_signature IS NOT NULL
            """
        ),
        {"catalog_version": catalog_version},
    ).fetchall()
    by_locator: dict[str, dict[str, object]] = {}
    by_signature: dict[str, dict[str, object]] = {}
    for row in rows:
        item = {
            "archive_id": row.id,
            "locator_key": row.locator_key,
            "locator_size_bytes": row.locator_size_bytes,
            "catalog_signature": row.catalog_signature,
        }
        by_locator.setdefault(row.locator_key, item)
        by_signature.setdefault(row.catalog_signature, item)
    return by_locator, by_signature


def _discover_seed_locators(scan_root: Path, max_workers: int):
    from e2ude_core.services.discovery import discover_archives

    result = discover_archives(scan_root, max_workers=max_workers)
    return [
        {
            "locator_key": _locator_key_for_path(archive.path),
            "locator_path": str(archive.path),
            "locator_size_bytes": archive.size_bytes,
            "archive": archive,
        }
        for archive in result.archives
    ], result.scanned_directory_count


def _load_destination_archives(conn, dest_schema: str, locator_keys: Sequence[str]):
    import sqlalchemy as sa

    rows: list[dict[str, object]] = []
    for i in range(0, len(locator_keys), 1000):
        batch = locator_keys[i : i + 1000]
        result = conn.execute(
            sa.text(
                f"""
                SELECT id, locator_key, locator_path, locator_size_bytes
                FROM {_mssql_name(dest_schema, "metadata_archive")}
                WHERE locator_key IN :locator_keys
                """
            ).bindparams(sa.bindparam("locator_keys", expanding=True)),
            {"locator_keys": batch},
        )
        rows.extend(
            {
                "dest_archive_id": row.id,
                "locator_key": row.locator_key,
                "locator_path": row.locator_path,
                "locator_size_bytes": row.locator_size_bytes,
            }
            for row in result
        )
    return rows


def _catalog_signature_for_zip(zip_path: Path) -> str:
    from e2ude_core.orchestration.catalog import archive_catalog_signature
    from e2ude_core.services.zip_io import iter_archive_members

    return archive_catalog_signature(iter_archive_members(zip_path))


def _build_catalog_seed_map(
    conn,
    *,
    source_schema: str,
    destination_archives: Sequence[dict[str, object]],
    catalog_version: int,
) -> tuple[list[dict[str, object]], dict[str, int]]:
    by_locator, by_signature = _source_catalog_archives(
        conn, source_schema, catalog_version=catalog_version
    )
    matches: list[dict[str, object]] = []
    stats = {
        "archives": len(destination_archives),
        "locator_matches": 0,
        "signature_matches": 0,
        "signature_reads": 0,
        "signature_errors": 0,
        "unmatched": 0,
    }

    for archive in destination_archives:
        locator_key = str(archive["locator_key"])
        locator_size = int(archive["locator_size_bytes"])
        source = by_locator.get(locator_key)
        if source is not None and source["locator_size_bytes"] == locator_size:
            stats["locator_matches"] += 1
            matches.append(
                {
                    "source_archive_id": source["archive_id"],
                    "dest_archive_id": archive.get("dest_archive_id"),
                    "catalog_signature": source["catalog_signature"],
                    "match": "locator",
                }
            )
            continue

        try:
            catalog_signature = _catalog_signature_for_zip(
                Path(archive["locator_path"])
            )
        except Exception:
            stats["signature_errors"] += 1
            stats["unmatched"] += 1
            continue

        stats["signature_reads"] += 1
        source = by_signature.get(catalog_signature)
        if source is None:
            stats["unmatched"] += 1
            continue

        stats["signature_matches"] += 1
        matches.append(
            {
                "source_archive_id": source["archive_id"],
                "dest_archive_id": archive.get("dest_archive_id"),
                "catalog_signature": catalog_signature,
                "match": "catalog_signature",
            }
        )
    return matches, stats


def _stage_catalog_seed_map(conn, rows: Sequence[dict[str, object]]) -> None:
    import sqlalchemy as sa

    conn.exec_driver_sql(
        "IF OBJECT_ID('tempdb..#catalog_seed_map') IS NOT NULL "
        "DROP TABLE #catalog_seed_map"
    )
    conn.exec_driver_sql(
        """
        CREATE TABLE #catalog_seed_map (
            source_archive_id int NOT NULL,
            dest_archive_id int NOT NULL,
            catalog_signature varchar(40) NOT NULL
        )
        """
    )
    if not rows:
        return
    stmt = sa.text(
        """
        INSERT INTO #catalog_seed_map (
            source_archive_id, dest_archive_id, catalog_signature
        )
        VALUES (
            :source_archive_id, :dest_archive_id, :catalog_signature
        )
        """
    )
    for i in range(0, len(rows), 10000):
        conn.execute(stmt, rows[i : i + 10000])


def _metadata_file_seed_plan_count(
    conn,
    *,
    source_schema: str,
    source_archive_ids: Sequence[int],
) -> int:
    import sqlalchemy as sa

    if not source_archive_ids:
        return 0
    if not _mssql_has_table(conn, source_schema, "metadata_file"):
        return 0
    if "archive_id" not in _mssql_columns(conn, source_schema, "metadata_file"):
        return 0
    total = 0
    for i in range(0, len(source_archive_ids), 1000):
        batch = source_archive_ids[i : i + 1000]
        total += conn.execute(
            sa.text(
                f"""
                SELECT COUNT(*)
                FROM {_mssql_name(source_schema, "metadata_file")}
                WHERE archive_id IN :archive_ids
                """
            ).bindparams(sa.bindparam("archive_ids", expanding=True)),
            {"archive_ids": batch},
        ).scalar_one()
    return total


def _seed_metadata_file(
    conn,
    *,
    source_schema: str,
    dest_schema: str,
    catalog_version: int,
) -> int:
    import sqlalchemy as sa

    required = {
        "archive_id",
        "content_hash",
        "relative_path",
        "file_size_bytes",
        "compressed_size_bytes",
        "crc32",
        "zip_depth",
    }
    if not required.issubset(_mssql_columns(conn, source_schema, "metadata_file")):
        return 0

    result = conn.execute(
        sa.text(
            f"""
            INSERT INTO {_mssql_name(dest_schema, "metadata_file")} (
                archive_id,
                content_hash,
                relative_path,
                file_size_bytes,
                compressed_size_bytes,
                crc32,
                zip_depth
            )
            SELECT
                seed.dest_archive_id,
                source.content_hash,
                source.relative_path,
                source.file_size_bytes,
                source.compressed_size_bytes,
                source.crc32,
                source.zip_depth
            FROM {_mssql_name(source_schema, "metadata_file")} AS source
            INNER JOIN #catalog_seed_map AS seed
                ON seed.source_archive_id = source.archive_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM {_mssql_name(dest_schema, "metadata_file")} AS dest
                WHERE dest.archive_id = seed.dest_archive_id
                  AND dest.relative_path = source.relative_path
            )
            """
        )
    )
    conn.execute(
        sa.text(
            f"""
            UPDATE archive
            SET
                cataloged_at = SYSUTCDATETIME(),
                catalog_version = :catalog_version,
                catalog_signature = seed.catalog_signature
            FROM {_mssql_name(dest_schema, "metadata_archive")} AS archive
            INNER JOIN #catalog_seed_map AS seed
                ON seed.dest_archive_id = archive.id
            """
        ),
        {"catalog_version": catalog_version},
    )
    return _rowcount(result)


def _source_manifest_target_tables(
    conn,
    *,
    source_schema: str,
    artifact_key: str,
    parser_version: int,
) -> list[str]:
    import sqlalchemy as sa

    required = {
        "content_hash",
        "artifact_key",
        "target_table",
        "parser_version",
        "row_count",
    }
    if not required.issubset(
        _mssql_columns(conn, source_schema, "metadata_artifact_manifest")
    ):
        return []
    rows = conn.execute(
        sa.text(
            f"""
            SELECT DISTINCT target_table
            FROM {_mssql_name(source_schema, "metadata_artifact_manifest")}
            WHERE artifact_key = :artifact_key
              AND parser_version >= :parser_version
            ORDER BY target_table
            """
        ),
        {"artifact_key": artifact_key, "parser_version": parser_version},
    ).fetchall()
    return [row.target_table for row in rows]


def _leaf_insert_columns(
    conn,
    *,
    source_schema: str,
    source_table: str,
    model,
    require_dest_table: bool,
):
    dest_schema = model.__table__.schema
    dest_table = model.__tablename__
    if dest_schema is None:
        return None, "destination table has no schema"
    if not _mssql_has_table(conn, source_schema, source_table):
        return None, "source table missing"
    if require_dest_table and not _mssql_has_table(conn, dest_schema, dest_table):
        return None, "destination table missing"
    source_columns = _mssql_columns(conn, source_schema, source_table)
    model_columns = [column.name for column in model.__table__.columns]
    dest_columns = (
        _mssql_columns(conn, dest_schema, dest_table)
        if require_dest_table
        else set(model_columns)
    )
    missing = [
        column
        for column in model_columns
        if column not in source_columns or column not in dest_columns
    ]
    if missing:
        return None, f"missing columns: {', '.join(missing[:5])}"
    if "content_hash" not in model_columns:
        return None, "content_hash missing"
    return model_columns, ""


def _count_reusable_leaf_rows(
    conn,
    *,
    source_schema: str,
    source_table: str,
    artifact_key: str,
    parser_version: int,
) -> int:
    import sqlalchemy as sa

    return conn.execute(
        sa.text(
            f"""
            SELECT COUNT(*)
            FROM {_mssql_name(source_schema, source_table)} AS source
            INNER JOIN {_mssql_name(source_schema, "metadata_artifact_manifest")} AS manifest
                ON manifest.content_hash = source.content_hash
               AND manifest.artifact_key = :artifact_key
               AND manifest.target_table = :source_table
               AND manifest.parser_version >= :parser_version
            WHERE EXISTS (
                SELECT 1
                FROM {_mssql_name(source_schema, "metadata_file")} AS source_file
                INNER JOIN #catalog_seed_map AS seed
                    ON seed.source_archive_id = source_file.archive_id
                WHERE source_file.content_hash = source.content_hash
            )
            """
        ),
        {
            "artifact_key": artifact_key,
            "source_table": source_table,
            "parser_version": parser_version,
        },
    ).scalar_one()


def _seed_leaf_table(
    conn,
    *,
    source_schema: str,
    source_table: str,
    dest_schema: str,
    dest_table: str,
    columns: Sequence[str],
    artifact_key: str,
    parser_version: int,
) -> int:
    import sqlalchemy as sa

    column_list = _mssql_column_list(columns)
    result = conn.execute(
        sa.text(
            f"""
            INSERT INTO {_mssql_name(dest_schema, dest_table)} ({column_list})
            SELECT {", ".join(f"source.{_quote_mssql_identifier(column)}" for column in columns)}
            FROM {_mssql_name(source_schema, source_table)} AS source
            INNER JOIN {_mssql_name(source_schema, "metadata_artifact_manifest")} AS manifest
                ON manifest.content_hash = source.content_hash
               AND manifest.artifact_key = :artifact_key
               AND manifest.target_table = :source_table
               AND manifest.parser_version >= :parser_version
            WHERE NOT EXISTS (
                SELECT 1
                FROM {_mssql_name(dest_schema, dest_table)} AS dest
                WHERE dest.content_hash = source.content_hash
            )
              AND EXISTS (
                  SELECT 1
                  FROM {_mssql_name(dest_schema, "metadata_file")} AS dest_file
                  WHERE dest_file.content_hash = source.content_hash
              )
            """
        ),
        {
            "artifact_key": artifact_key,
            "source_table": source_table,
            "parser_version": parser_version,
        },
    )
    return _rowcount(result)


def _seed_manifest_rows(
    conn,
    *,
    source_schema: str,
    source_table: str,
    dest_schema: str,
    dest_table: str,
    artifact_key: str,
    parser_version: int,
) -> int:
    import sqlalchemy as sa

    result = conn.execute(
        sa.text(
            f"""
            INSERT INTO {_mssql_name(dest_schema, "metadata_artifact_manifest")} (
                content_hash,
                artifact_key,
                target_table,
                parser_version,
                row_count,
                created_at
            )
            SELECT
                source.content_hash,
                :artifact_key,
                :dest_table,
                :parser_version,
                source.row_count,
                SYSUTCDATETIME()
            FROM {_mssql_name(source_schema, "metadata_artifact_manifest")} AS source
            WHERE source.artifact_key = :artifact_key
              AND source.target_table = :source_table
              AND source.parser_version >= :parser_version
              AND EXISTS (
                  SELECT 1
                  FROM {_mssql_name(dest_schema, dest_table)} AS leaf
                  WHERE leaf.content_hash = source.content_hash
              )
              AND EXISTS (
                  SELECT 1
                  FROM {_mssql_name(dest_schema, "metadata_file")} AS dest_file
                  WHERE dest_file.content_hash = source.content_hash
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {_mssql_name(dest_schema, "metadata_artifact_manifest")} AS dest
                  WHERE dest.content_hash = source.content_hash
                    AND dest.artifact_key = :artifact_key
              )
            """
        ),
        {
            "artifact_key": artifact_key,
            "source_table": source_table,
            "dest_table": dest_table,
            "parser_version": parser_version,
        },
    )
    return _rowcount(result)


def _artifact_seed_plan(
    conn,
    *,
    source_schema: str,
    dest_schema: str,
    apply: bool,
) -> tuple[list[list[object]], int, int]:
    import sqlalchemy as sa

    from e2ude_core.runtime_files import artifact_key_for

    manifest_columns = _mssql_columns(conn, source_schema, "metadata_artifact_manifest")
    manifest_ready = {
        "content_hash",
        "artifact_key",
        "target_table",
        "parser_version",
        "row_count",
    }.issubset(manifest_columns)
    artifact_plan: list[list[object]] = []
    leaf_total = 0
    manifest_total = 0
    for spec in _handled_specs():
        for model in spec.expected_models:
            artifact_key = artifact_key_for(spec, model)
            dest_table = model.__tablename__
            if not manifest_ready:
                artifact_plan.append(
                    [
                        artifact_key,
                        "-",
                        dest_table,
                        "skip",
                        "source manifest missing",
                        0,
                        0,
                    ]
                )
                continue
            source_candidates = _source_manifest_target_tables(
                conn,
                source_schema=source_schema,
                artifact_key=artifact_key,
                parser_version=spec.version or 0,
            )
            if dest_table not in source_candidates:
                source_candidates.insert(0, dest_table)

            copied_for_artifact = False
            for source_table in source_candidates:
                columns, reason = _leaf_insert_columns(
                    conn,
                    source_schema=source_schema,
                    source_table=source_table,
                    model=model,
                    require_dest_table=apply,
                )
                if columns is None:
                    artifact_plan.append(
                        [
                            artifact_key,
                            source_table,
                            dest_table,
                            "skip",
                            reason,
                            0,
                            0,
                        ]
                    )
                    continue

                if apply:
                    leaf_rows = _seed_leaf_table(
                        conn,
                        source_schema=source_schema,
                        source_table=source_table,
                        dest_schema=dest_schema,
                        dest_table=dest_table,
                        columns=columns,
                        artifact_key=artifact_key,
                        parser_version=spec.version or 0,
                    )
                    manifest_rows = _seed_manifest_rows(
                        conn,
                        source_schema=source_schema,
                        source_table=source_table,
                        dest_schema=dest_schema,
                        dest_table=dest_table,
                        artifact_key=artifact_key,
                        parser_version=spec.version or 0,
                    )
                else:
                    leaf_rows = _count_reusable_leaf_rows(
                        conn,
                        source_schema=source_schema,
                        source_table=source_table,
                        artifact_key=artifact_key,
                        parser_version=spec.version or 0,
                    )
                    manifest_rows = 0
                    if leaf_rows:
                        manifest_rows = conn.execute(
                            sa.text(
                                f"""
                                SELECT COUNT(*)
                                FROM {_mssql_name(source_schema, "metadata_artifact_manifest")} AS manifest
                                WHERE manifest.artifact_key = :artifact_key
                                  AND manifest.target_table = :source_table
                                  AND manifest.parser_version >= :parser_version
                                  AND EXISTS (
                                      SELECT 1
                                      FROM {_mssql_name(source_schema, "metadata_file")} AS source_file
                                      INNER JOIN #catalog_seed_map AS seed
                                          ON seed.source_archive_id = source_file.archive_id
                                      WHERE source_file.content_hash = manifest.content_hash
                                  )
                                """
                            ),
                            {
                                "artifact_key": artifact_key,
                                "source_table": source_table,
                                "parser_version": spec.version or 0,
                            },
                        ).scalar_one()

                if (
                    not leaf_rows
                    and not manifest_rows
                    and source_table != source_candidates[-1]
                ):
                    continue

                leaf_total += int(leaf_rows)
                manifest_total += int(manifest_rows)
                artifact_plan.append(
                    [
                        artifact_key,
                        source_table,
                        dest_table,
                        "copy" if leaf_rows or manifest_rows else "empty",
                        "",
                        leaf_rows,
                        manifest_rows,
                    ]
                )
                copied_for_artifact = True
                break

            if not copied_for_artifact and not source_candidates:
                artifact_plan.append(
                    [
                        artifact_key,
                        "-",
                        dest_table,
                        "skip",
                        "source manifest missing",
                        0,
                        0,
                    ]
                )
    return artifact_plan, leaf_total, manifest_total


def cmd_schema_seed(args) -> int:
    source_schema = _resolve_schema_ref(args.source_schema)
    dest_schema = _resolve_schema_ref(args.dest_schema)
    if source_schema == dest_schema:
        raise SystemExit("--from and --to must be different schemas.")
    if not args.plan and not args.yes:
        raise SystemExit("Use --plan to preview or --yes to seed.")

    _apply_schema_command_env(args, dest_schema)

    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.schema_safety import is_protected_schema
    from e2ude_core.db.setup import initialize_database, register_archives_bulk
    from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION

    if settings.database.type != "mssql":
        raise SystemExit("schema seed only supports MSSQL targets.")
    if is_protected_schema(dest_schema):
        raise SystemExit(f"Refusing to seed protected schema [{dest_schema}].")
    if settings.paths.scan_root is None:
        raise SystemExit("scan_root is not configured.")

    _print_target(_target_info(f"schema seed {source_schema} -> {dest_schema}"))
    eng = get_engine(settings.database)
    try:
        with eng.connect() as conn:
            if not _schema_exists(conn, source_schema):
                raise SystemExit(f"Source schema [{source_schema}] does not exist.")
            source_tables = _fetch_schema_tables(conn, source_schema)
            dest_exists = _schema_exists(conn, dest_schema)
            dest_tables = _fetch_schema_tables(conn, dest_schema) if dest_exists else []

        print(f"Source [{source_schema}]: {len(source_tables)} tables")
        if args.plan:
            print(
                f"Destination [{dest_schema}]: "
                f"{len(dest_tables) if dest_exists else 'will create'}"
            )
        else:
            print(f"Destination [{dest_schema}]: initializing runtime tables")
            initialize_database(eng, reset_tables=False)

        locators, scanned_dirs = _discover_seed_locators(
            settings.paths.scan_root,
            settings.runtime.discovery_workers,
        )
        print(f"Archive locators discovered {len(locators)}")
        print(f"Directories scanned          {scanned_dirs}")

        if args.plan:
            destination_archives = [
                {
                    "locator_key": item["locator_key"],
                    "locator_path": item["locator_path"],
                    "locator_size_bytes": item["locator_size_bytes"],
                }
                for item in locators
            ]
        else:
            archive_map = register_archives_bulk(
                eng,
                [item["archive"] for item in locators],
            )
            locator_keys = [str(item["locator_key"]) for item in locators]
            with eng.connect() as conn:
                destination_archives = _load_destination_archives(
                    conn, dest_schema, locator_keys
                )
            print(f"Archive locators registered  {len(archive_map)}")

        with eng.connect() as conn:
            matches, catalog_stats = _build_catalog_seed_map(
                conn,
                source_schema=source_schema,
                destination_archives=destination_archives,
                catalog_version=CURRENT_ARCHIVE_CATALOG_VERSION,
            )
            source_archive_ids = [int(match["source_archive_id"]) for match in matches]
            metadata_file_rows = _metadata_file_seed_plan_count(
                conn,
                source_schema=source_schema,
                source_archive_ids=source_archive_ids,
            )
            if args.plan:
                _stage_catalog_seed_map(
                    conn,
                    [
                        {
                            "source_archive_id": row["source_archive_id"],
                            "dest_archive_id": 0,
                            "catalog_signature": row["catalog_signature"],
                        }
                        for row in matches
                    ],
                )
                artifact_plan, leaf_total, manifest_total = _artifact_seed_plan(
                    conn,
                    source_schema=source_schema,
                    dest_schema=dest_schema,
                    apply=False,
                )

        if not args.plan:
            with eng.begin() as conn:
                seed_rows = [
                    row for row in matches if row.get("dest_archive_id") is not None
                ]
                _stage_catalog_seed_map(conn, seed_rows)
                metadata_file_rows = _seed_metadata_file(
                    conn,
                    source_schema=source_schema,
                    dest_schema=dest_schema,
                    catalog_version=CURRENT_ARCHIVE_CATALOG_VERSION,
                )
                artifact_plan, leaf_total, manifest_total = _artifact_seed_plan(
                    conn,
                    source_schema=source_schema,
                    dest_schema=dest_schema,
                    apply=True,
                )

        print("")
        print("Catalog")
        _print_table(
            ["metric", "count"],
            [
                ["destination archives", catalog_stats["archives"]],
                ["locator matches", catalog_stats["locator_matches"]],
                ["signature matches", catalog_stats["signature_matches"]],
                ["signature reads", catalog_stats["signature_reads"]],
                ["signature errors", catalog_stats["signature_errors"]],
                ["unmatched", catalog_stats["unmatched"]],
                [
                    f"metadata_file rows {'reusable' if args.plan else 'seeded'}",
                    metadata_file_rows,
                ],
            ],
        )

        print("")
        print("Artifacts")
        if artifact_plan:
            _print_table(
                [
                    "artifact",
                    "source_table",
                    "dest_table",
                    "action",
                    "reason",
                    "leaf_rows",
                    "manifest_rows",
                ],
                artifact_plan,
            )
        print("")
        print(
            f"leaf rows {'reusable' if args.plan else 'seeded'}          {leaf_total}"
        )
        print(
            f"manifest rows {'reusable' if args.plan else 'seeded'}      {manifest_total}"
        )

        if args.plan:
            print("")
            print("Plan only. No changes were made.")
    finally:
        eng.dispose()
    return 0


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
            if args.preview:
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
            if args.preview:
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
    parser.add_argument("--env", choices=sorted(ENV_SCHEMAS), help=TARGET_HELP["env"])
    parser.add_argument("--schema", help=TARGET_HELP["schema"])
    parser.add_argument("--sqlite", type=Path, help=TARGET_HELP["sqlite"])
    parser.add_argument("--config", type=Path, help=TARGET_HELP["config"])


def add_mssql_connection_args(parser) -> None:
    parser.add_argument("--config", type=Path, help=TARGET_HELP["config"])


def add_work_args(parser) -> None:
    add_target_args(parser)
    parser.add_argument("parser", nargs="?", help="Parser id, file type, or prefix")
    parser.add_argument(
        "--from-file",
        type=Path,
        help=(
            "Infer the parser from a local example file, then run matching "
            "cataloged work. Does not parse that local file."
        ),
    )
    parser.add_argument("--limit", type=int, help="Maximum distinct hashes to process")
    parser.add_argument("--plan", action="store_true", help="Show work without writing")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild current parser outputs even if they are marked complete.",
    )
    parser.add_argument("--staging-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--failure-dir", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--keep-staging", action="store_true", help=argparse.SUPPRESS)


def _add_parser_preview_command(subparsers, name: str, *, help_text: str | None = None):
    command = subparsers.add_parser(
        name,
        help=help_text,
        description=PARSER_PREVIEW_HELP,
        formatter_class=HELP_FORMATTER,
    )
    command.add_argument("file_path", type=Path, help="Local file to parse.")
    command.add_argument("--as", dest="as_parser", help="Parser id to use explicitly.")
    command.add_argument("--head", type=int, default=5, help="Preview row count.")
    command.set_defaults(func=cmd_preview)
    return command


def _add_parser_work_command(
    subparsers,
    name: str,
    *,
    description: str,
    func,
    help_text: str | None = None,
):
    command = subparsers.add_parser(
        name,
        help=help_text,
        description=description,
        formatter_class=HELP_FORMATTER,
    )
    add_work_args(command)
    command.set_defaults(func=func)
    return command


def _add_invalidate_command(subparsers, name: str, *, help_text: str | None = None):
    command = subparsers.add_parser(
        name,
        help=help_text,
        description=PARSER_INVALIDATE_HELP,
        formatter_class=HELP_FORMATTER,
    )
    add_target_args(command)
    command.add_argument("parser", help="Parser id, file type, or prefix.")
    command.add_argument(
        "--content-hash",
        action="append",
        type=_parse_content_hash,
        help="Limit to one 32-character content hash. Repeatable.",
    )
    command.add_argument(
        "--plan", action="store_true", help="Show work without writing."
    )
    command.add_argument("--yes", action="store_true", help="Confirm invalidation.")
    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="e2ude",
        description="Catalog and parse RSM transport archives.",
        epilog=TOP_LEVEL_HELP,
        formatter_class=HELP_FORMATTER,
    )
    sub = parser.add_subparsers(
        dest="command_name",
        required=True,
        metavar="{refresh,parser,schema}",
    )

    refresh = sub.add_parser(
        "refresh",
        help="Discover archives and process pending parser work.",
        description=REFRESH_HELP,
        formatter_class=HELP_FORMATTER,
    )
    add_target_args(refresh)
    refresh.add_argument(
        "--preview", action="store_true", help="Show target without writing."
    )
    refresh.add_argument(
        "--confirm", help="Required exact schema name for prod writes."
    )
    refresh.add_argument("--staging-root", type=Path, help=argparse.SUPPRESS)
    refresh.set_defaults(func=cmd_refresh)

    parser_cmd = sub.add_parser(
        "parser",
        help="List, preview, backfill, and invalidate parser outputs.",
        description=PARSER_HELP,
        formatter_class=HELP_FORMATTER,
    )
    parser_sub = parser_cmd.add_subparsers(
        dest="parser_command",
        required=True,
        metavar="{list,status,preview,backfill,invalidate}",
    )

    parser_list = parser_sub.add_parser(
        "list",
        help="Show parser ids, file patterns, and output tables.",
        description=PARSER_LIST_HELP,
        formatter_class=HELP_FORMATTER,
    )
    add_target_args(parser_list)
    parser_list.add_argument(
        "--counts", action="store_true", help="Include catalog and output counts."
    )
    parser_list.set_defaults(func=cmd_parsers)

    parser_status = parser_sub.add_parser(
        "status",
        help="Show parser coverage and remaining work.",
        description=PARSER_STATUS_HELP,
        formatter_class=HELP_FORMATTER,
    )
    add_target_args(parser_status)
    parser_status.add_argument(
        "parser", nargs="?", help="Optional parser id, file type, or prefix."
    )
    parser_status.set_defaults(func=cmd_parser_status)

    _add_parser_preview_command(
        parser_sub,
        "preview",
        help_text="Parse one local file without touching the database.",
    )

    _add_parser_work_command(
        parser_sub,
        "backfill",
        description=PARSER_BACKFILL_HELP,
        func=cmd_parser_backfill,
        help_text="Run one parser across cataloged history.",
    )

    parser_invalidate = _add_invalidate_command(
        parser_sub,
        "invalidate",
        help_text="Mark one parser's outputs stale.",
    )
    parser_invalidate.set_defaults(func=cmd_parser_invalidate)

    schema = sub.add_parser(
        "schema",
        help="Check, promote, and clean up MSSQL schemas.",
        description=SCHEMA_HELP,
        formatter_class=HELP_FORMATTER,
    )
    schema_sub = schema.add_subparsers(dest="schema_command", required=True)

    cleanup = schema_sub.add_parser("cleanup")
    add_mssql_connection_args(cleanup)
    cleanup.add_argument("schema_name", nargs="?")
    cleanup.add_argument("--schema")
    cleanup.add_argument("--preview", action="store_true")
    cleanup.add_argument("--yes", action="store_true")
    cleanup.add_argument("--confirm-schema")
    cleanup.add_argument("--keep-schema", action="store_true")
    cleanup.set_defaults(func=cmd_schema_cleanup)

    check = schema_sub.add_parser("check")
    add_mssql_connection_args(check)
    check.add_argument("schema")
    check.set_defaults(func=cmd_schema_check)

    seed = schema_sub.add_parser(
        "seed",
        help="Warm a fresh schema from reusable catalog and parser rows.",
        description=SCHEMA_SEED_HELP,
        formatter_class=HELP_FORMATTER,
    )
    add_mssql_connection_args(seed)
    seed.add_argument("--from", dest="source_schema", required=True)
    seed.add_argument("--to", dest="dest_schema", required=True)
    seed.add_argument("--plan", action="store_true")
    seed.add_argument("--yes", action="store_true")
    seed.set_defaults(func=cmd_schema_seed)

    promote = schema_sub.add_parser("promote")
    add_mssql_connection_args(promote)
    promote.add_argument("source", nargs="?")
    promote.add_argument("dest", nargs="?", default="prod")
    promote.add_argument("--source", dest="source_option")
    promote.add_argument("--dest", dest="dest_option")
    promote.add_argument("--archive")
    promote.add_argument("--preview", action="store_true")
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
