from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath

import sqlalchemy as sa

from e2ude_core.db.schema_safety import (
    is_protected_schema,
    require_exact_confirmation,
    validate_schema_name,
)


CONTROL_TABLES = {
    "metadata_archive",
    "metadata_discovery_directory",
    "metadata_hash_registry",
    "metadata_file",
    "metadata_artifact_manifest",
    "processing_sessions",
    "processing_jobs",
}

AUDIT_TABLES = {"processing_sessions", "processing_jobs"}
REQUIRED_LEGACY_TABLES = {
    "metadata_archive",
    "metadata_folder",
    "metadata_hash_registry",
    "metadata_file",
}
ARCHIVE_NAME_PATTERN = re.compile(r"([0-9]+)_([0-9]{8}_[0-9]{6})")


def _clean_path_text(value: object) -> str:
    return str(value or "").strip()


def _path_key(value: object) -> str:
    text = _clean_path_text(value)
    return str(PureWindowsPath(text)).casefold() if text else ""


def _archive_name_key(value: object) -> str:
    text = _clean_path_text(value)
    return PureWindowsPath(text).name.casefold() if text else ""


def _mapping_key(value: object, method: str) -> str:
    if method == "exact-path":
        return _path_key(value)
    return _archive_name_key(value)


def _parse_archive_path(value: object) -> tuple[str, datetime] | None:
    name = PureWindowsPath(_clean_path_text(value)).name
    match = ARCHIVE_NAME_PATTERN.search(name)
    if not match:
        return None

    buno, dt_str = match.groups()
    try:
        return buno, datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def _qname(schema_name: str, table_name: str) -> str:
    return f"[{schema_name}].[{table_name}]"


def _qcol(column_name: str) -> str:
    return f"[{column_name}]"


def _schema_exists(conn: sa.Connection, schema_name: str) -> bool:
    return (
        conn.execute(
            sa.text("SELECT 1 FROM sys.schemas WHERE name = :schema_name"),
            {"schema_name": schema_name},
        ).scalar_one_or_none()
        is not None
    )


def _schema_tables(conn: sa.Connection, schema_name: str) -> set[str]:
    rows = conn.execute(
        sa.text(
            """
            SELECT t.name
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name
            """
        ),
        {"schema_name": schema_name},
    ).fetchall()
    return {row.name for row in rows}


def _table_count(conn: sa.Connection, schema_name: str, table_name: str) -> int:
    return conn.execute(
        sa.text(f"SELECT COUNT(*) FROM {_qname(schema_name, table_name)}")
    ).scalar_one()


def _column_types(
    conn: sa.Connection, schema_name: str, table_name: str
) -> dict[str, str]:
    rows = conn.execute(
        sa.text(
            """
            SELECT c.name AS column_name, ty.name AS type_name
            FROM sys.columns AS c
            INNER JOIN sys.tables AS t ON t.object_id = c.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
            WHERE s.name = :schema_name AND t.name = :table_name
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    ).fetchall()
    return {row.column_name: row.type_name for row in rows}


def _print_table(headers: list[str], rows: list[list[object]]) -> None:
    if not rows:
        print("(none)")
        return
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


def _drop_schema_tables(
    conn: sa.Connection, schema_name: str, table_names: set[str]
) -> None:
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
    for table_name in sorted(table_names):
        conn.execute(sa.text(f"DROP TABLE {_qname(schema_name, table_name)}"))


def _source_table_counts(
    conn: sa.Connection, source_schema: str, source_tables: set[str]
) -> list[list[object]]:
    rows = []
    for table_name in sorted(source_tables):
        rows.append([table_name, _table_count(conn, source_schema, table_name)])
    return rows


def _invalid_md5_count(conn: sa.Connection, source_schema: str) -> int:
    md5_type = _column_types(conn, source_schema, "metadata_hash_registry").get(
        "md5", ""
    )
    if md5_type.casefold() in {"binary", "varbinary"}:
        return 0

    return conn.execute(
        sa.text(
            f"""
            SELECT COUNT(*)
            FROM {_qname(source_schema, "metadata_hash_registry")}
            WHERE md5 IS NULL
               OR LEN(md5) <> 32
               OR md5 LIKE '%[^0-9A-Fa-f]%'
            """
        )
    ).scalar_one()


def _file_counts_by_folder(
    conn: sa.Connection, source_schema: str
) -> dict[int | None, tuple[int, int]]:
    rows = conn.execute(
        sa.text(
            f"""
            SELECT
                folder_id,
                COUNT(*) AS file_count,
                MIN(id) AS first_file_id
            FROM {_qname(source_schema, "metadata_file")} AS mf
            GROUP BY folder_id
            """
        )
    ).fetchall()
    return {
        row.folder_id: (row.file_count or 0, row.first_file_id or 0) for row in rows
    }


def _folder_paths(conn: sa.Connection, source_schema: str) -> dict[int, str]:
    rows = conn.execute(
        sa.text(
            f"""
            SELECT FolderID, FolderPath
            FROM {_qname(source_schema, "metadata_folder")}
            """
        )
    ).fetchall()
    return {row.FolderID: row.FolderPath for row in rows}


def _archive_paths(conn: sa.Connection, source_schema: str) -> dict[int, str]:
    rows = conn.execute(
        sa.text(
            f"""
            SELECT id, source_path
            FROM {_qname(source_schema, "metadata_archive")}
            ORDER BY id
            """
        )
    ).fetchall()
    return {row.id: row.source_path for row in rows}


def _folder_mapping_from_rows(
    *,
    file_counts: dict[int | None, tuple[int, int]],
    folder_paths: dict[int, str],
    archive_paths: dict[int, str],
    method: str,
    example_limit: int = 20,
) -> tuple[dict[str, int], dict[int, int], list[list[object]]]:
    archive_lookup = defaultdict(list)
    for archive_id, source_path in archive_paths.items():
        key = _mapping_key(source_path, method)
        if key:
            archive_lookup[key].append(archive_id)

    counts = {
        "total_files": 0,
        "missing_folder": 0,
        "missing_archive": 0,
        "ambiguous_archive": 0,
    }
    folder_archive_ids = {}
    examples = []
    sorted_file_counts = sorted(
        file_counts.items(), key=lambda item: (item[1][1], item[0] or 0)
    )

    for folder_id, (file_count, first_file_id) in sorted_file_counts:
        counts["total_files"] += file_count
        folder_path = folder_paths.get(folder_id) if folder_id is not None else None
        if folder_path is None:
            counts["missing_folder"] += file_count
            if len(examples) < example_limit:
                examples.append([first_file_id, folder_id, None, None, None])
            continue

        folder_key = _mapping_key(folder_path, method)
        archive_matches = archive_lookup.get(folder_key, [])
        if not archive_matches:
            counts["missing_archive"] += file_count
            if len(examples) < example_limit:
                examples.append([first_file_id, folder_id, folder_path, folder_key, 0])
            continue
        if len(archive_matches) > 1:
            counts["ambiguous_archive"] += file_count
            if len(examples) < example_limit:
                examples.append(
                    [
                        first_file_id,
                        folder_id,
                        folder_path,
                        folder_key,
                        len(archive_matches),
                    ]
                )
            continue

        folder_archive_ids[folder_id] = archive_matches[0]

    return counts, folder_archive_ids, examples


def _legacy_folder_mapping_from_rows(
    *,
    file_counts: dict[int | None, tuple[int, int]],
    folder_paths: dict[int, str],
    scanner_version: int,
    handler_generation: str,
    example_limit: int = 20,
) -> tuple[dict[str, int], dict[int, int], list[list[object]], list[dict[str, object]]]:
    seen_at = datetime.now(UTC).replace(tzinfo=None)
    counts = {
        "total_files": 0,
        "missing_folder": 0,
        "missing_archive": 0,
        "ambiguous_archive": 0,
    }
    parsed_rows = []
    examples = []
    sorted_file_counts = sorted(
        file_counts.items(), key=lambda item: (item[1][1], item[0] or 0)
    )

    for folder_id, (file_count, first_file_id) in sorted_file_counts:
        counts["total_files"] += file_count
        folder_path = folder_paths.get(folder_id) if folder_id is not None else None
        if folder_path is None:
            counts["missing_folder"] += file_count
            if len(examples) < example_limit:
                examples.append([first_file_id, folder_id, None, None, None])
            continue

        parsed = _parse_archive_path(folder_path)
        if parsed is None:
            counts["missing_archive"] += file_count
            if len(examples) < example_limit:
                examples.append(
                    [
                        first_file_id,
                        folder_id,
                        folder_path,
                        _archive_name_key(folder_path),
                        0,
                    ]
                )
            continue

        buno, archive_datetime = parsed
        parsed_rows.append(
            {
                "folder_id": folder_id,
                "file_count": file_count,
                "first_file_id": first_file_id,
                "source_path": _clean_path_text(folder_path),
                "archive_key": (buno, archive_datetime),
                "row": {
                    "id": folder_id,
                    "buno": buno,
                    "archive_datetime": archive_datetime,
                    "source_path": _clean_path_text(folder_path),
                    "source_size_bytes": 0,
                    "source_mtime_ns": 0,
                    "first_seen_at": seen_at,
                    "last_seen_at": seen_at,
                    "is_present": True,
                    "required_scan_version": scanner_version,
                    "completed_scan_version": scanner_version,
                    "required_handler_generation": handler_generation,
                    "completed_handler_generation": None,
                    "state": "NEEDS_PROCESSING",
                    "work_reason": "Seeded from legacy metadata_folder",
                },
            }
        )

    archive_key_counts = defaultdict(int)
    for item in parsed_rows:
        archive_key_counts[item["archive_key"]] += 1

    folder_archive_ids = {}
    archive_rows = []
    for item in parsed_rows:
        duplicate_count = archive_key_counts[item["archive_key"]]
        if duplicate_count > 1:
            counts["ambiguous_archive"] += item["file_count"]
            if len(examples) < example_limit:
                examples.append(
                    [
                        item["first_file_id"],
                        item["folder_id"],
                        item["source_path"],
                        item["archive_key"][0],
                        duplicate_count,
                    ]
                )
            continue

        folder_archive_ids[item["folder_id"]] = item["folder_id"]
        archive_rows.append(item["row"])

    return counts, folder_archive_ids, examples, archive_rows


def _folder_mapping_results(
    conn: sa.Connection,
    source_schema: str,
    *,
    scanner_version: int,
    handler_generation: str,
) -> dict[str, dict[str, object]]:
    file_counts = _file_counts_by_folder(conn, source_schema)
    folders = _folder_paths(conn, source_schema)
    archives = _archive_paths(conn, source_schema)
    results = {}
    for method in ("exact-path", "archive-name"):
        counts, folder_archive_ids, examples = _folder_mapping_from_rows(
            file_counts=file_counts,
            folder_paths=folders,
            archive_paths=archives,
            method=method,
        )
        results[method] = {
            "counts": counts,
            "folder_archive_ids": folder_archive_ids,
            "examples": examples,
            "archive_examples": [
                [archive_id, source_path, _mapping_key(source_path, method)]
                for archive_id, source_path in list(archives.items())[:10]
            ],
        }
    counts, folder_archive_ids, examples, archive_rows = (
        _legacy_folder_mapping_from_rows(
            file_counts=file_counts,
            folder_paths=folders,
            scanner_version=scanner_version,
            handler_generation=handler_generation,
        )
    )
    results["legacy-folder"] = {
        "counts": counts,
        "folder_archive_ids": folder_archive_ids,
        "examples": examples,
        "archive_examples": [
            [row["id"], row["source_path"], row["buno"]] for row in archive_rows[:10]
        ],
        "archive_rows": archive_rows,
    }
    return results


def _select_folder_mapping(
    requested_method: str, mapping_counts: dict[str, dict[str, int]]
) -> str:
    if requested_method != "auto":
        return requested_method

    for method in ("exact-path", "archive-name", "legacy-folder"):
        counts = mapping_counts[method]
        if (
            counts["missing_folder"] == 0
            and counts["missing_archive"] == 0
            and counts["ambiguous_archive"] == 0
        ):
            return method

    return min(
        ("exact-path", "archive-name", "legacy-folder"),
        key=lambda method: (
            mapping_counts[method]["missing_archive"],
            mapping_counts[method]["ambiguous_archive"],
        ),
    )


def _required_columns_missing(table, source_columns: set[str]) -> list[str]:
    missing = []
    for column in table.columns:
        if column.name in source_columns:
            continue
        has_default = column.default is not None or column.server_default is not None
        if not column.nullable and not has_default and not column.autoincrement:
            missing.append(column.name)
    return missing


def _compatible_common_columns(table, source_columns: set[str]) -> list[str]:
    return [column.name for column in table.columns if column.name in source_columns]


def _uses_identity_insert(table, column_names: list[str]) -> bool:
    if "id" not in column_names:
        return False
    id_column = table.columns.get("id")
    return bool(id_column is not None and id_column.primary_key)


def _table_compatibility(
    runtime_tables: list[sa.Table],
    conn: sa.Connection,
    source_schema: str,
    source_tables: set[str],
) -> tuple[dict[str, sa.Table], list[list[object]]]:
    compatible = {}
    rows = []
    for table in runtime_tables:
        if table.name in CONTROL_TABLES:
            continue
        if table.name not in source_tables:
            rows.append([table.name, "skip", "missing source table"])
            continue
        source_columns = set(_column_types(conn, source_schema, table.name))
        missing = _required_columns_missing(table, source_columns)
        if missing:
            rows.append([table.name, "skip", f"missing columns: {', '.join(missing)}"])
            continue
        compatible[table.name] = table
        rows.append([table.name, "copy", _table_count(conn, source_schema, table.name)])
    return compatible, rows


def _set_identity_insert(
    conn: sa.Connection, schema_name: str, table_name: str, enabled: bool
) -> None:
    state = "ON" if enabled else "OFF"
    conn.execute(
        sa.text(f"SET IDENTITY_INSERT {_qname(schema_name, table_name)} {state}")
    )


def _copy_common_table(
    conn: sa.Connection,
    *,
    source_schema: str,
    dest_schema: str,
    table,
) -> int:
    source_columns = set(_column_types(conn, source_schema, table.name))
    columns = _compatible_common_columns(table, source_columns)
    if not columns:
        return 0

    quoted_columns = ", ".join(_qcol(name) for name in columns)
    identity_insert = _uses_identity_insert(table, columns)
    if identity_insert:
        _set_identity_insert(conn, dest_schema, table.name, True)
    try:
        result = conn.execute(
            sa.text(
                f"INSERT INTO {_qname(dest_schema, table.name)} ({quoted_columns}) "
                f"SELECT {quoted_columns} FROM {_qname(source_schema, table.name)}"
            )
        )
    finally:
        if identity_insert:
            _set_identity_insert(conn, dest_schema, table.name, False)
    return result.rowcount or 0


def _copy_hash_registry(
    conn: sa.Connection, source_schema: str, dest_schema: str
) -> int:
    md5_type = _column_types(conn, source_schema, "metadata_hash_registry").get(
        "md5", ""
    )
    md5_expr = (
        "[md5]"
        if md5_type.casefold() in {"binary", "varbinary"}
        else "CONVERT(varbinary(16), [md5], 2)"
    )
    _set_identity_insert(conn, dest_schema, "metadata_hash_registry", True)
    try:
        result = conn.execute(
            sa.text(
                f"""
                INSERT INTO {_qname(dest_schema, "metadata_hash_registry")} ([id], [md5])
                SELECT [id], {md5_expr}
                FROM {_qname(source_schema, "metadata_hash_registry")}
                """
            )
        )
    finally:
        _set_identity_insert(conn, dest_schema, "metadata_hash_registry", False)
    return result.rowcount or 0


def _copy_metadata_archive_from_folders(
    conn: sa.Connection, dest_schema: str, archive_rows: list[dict[str, object]]
) -> int:
    if not archive_rows:
        return 0

    columns = [
        "id",
        "buno",
        "archive_datetime",
        "source_path",
        "source_size_bytes",
        "source_mtime_ns",
        "first_seen_at",
        "last_seen_at",
        "is_present",
        "required_scan_version",
        "completed_scan_version",
        "required_handler_generation",
        "completed_handler_generation",
        "state",
        "work_reason",
    ]
    quoted_columns = ", ".join(_qcol(name) for name in columns)
    value_columns = ", ".join(f":{name}" for name in columns)

    _set_identity_insert(conn, dest_schema, "metadata_archive", True)
    try:
        result = conn.execute(
            sa.text(
                f"""
                INSERT INTO {_qname(dest_schema, "metadata_archive")}
                    ({quoted_columns})
                VALUES ({value_columns})
                """
            ),
            archive_rows,
        )
    finally:
        _set_identity_insert(conn, dest_schema, "metadata_archive", False)
    return result.rowcount or 0


def _copy_metadata_file(
    conn: sa.Connection,
    source_schema: str,
    dest_schema: str,
    folder_archive_ids: dict[int, int],
) -> int:
    if not folder_archive_ids:
        return 0

    conn.execute(
        sa.text(
            """
            IF OBJECT_ID('tempdb..#e2ude_folder_archive_map') IS NOT NULL
                DROP TABLE #e2ude_folder_archive_map
            """
        )
    )
    conn.execute(
        sa.text(
            """
            CREATE TABLE #e2ude_folder_archive_map (
                folder_id int NOT NULL PRIMARY KEY,
                archive_id int NOT NULL
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO #e2ude_folder_archive_map (folder_id, archive_id)
            VALUES (:folder_id, :archive_id)
            """
        ),
        [
            {"folder_id": folder_id, "archive_id": archive_id}
            for folder_id, archive_id in folder_archive_ids.items()
        ],
    )

    _set_identity_insert(conn, dest_schema, "metadata_file", True)
    try:
        result = conn.execute(
            sa.text(
                f"""
                INSERT INTO {_qname(dest_schema, "metadata_file")}
                    ([id], [archive_id], [hash_id], [relative_path], [file_type], [file_size_bytes])
                SELECT
                    mf.[id],
                    da.[id],
                    mf.[hash_id],
                    mf.[relative_path],
                    mf.[file_type],
                    mf.[file_size_bytes]
                FROM {_qname(source_schema, "metadata_file")} AS mf
                INNER JOIN #e2ude_folder_archive_map AS m
                    ON m.folder_id = mf.folder_id
                INNER JOIN {_qname(dest_schema, "metadata_archive")} AS da
                    ON da.id = m.archive_id
                """
            )
        )
    finally:
        _set_identity_insert(conn, dest_schema, "metadata_file", False)
    return result.rowcount or 0


def _build_artifact_manifest(
    conn: sa.Connection,
    *,
    dest_schema: str,
    copied_leaf_tables: set[str],
    table_versions: dict[str, int],
) -> list[list[object]]:
    rows = []
    for table_name in sorted(copied_leaf_tables):
        handler_version = table_versions.get(table_name)
        if handler_version is None:
            rows.append([table_name, "skip", "no runtime handler version"])
            continue

        result = conn.execute(
            sa.text(
                f"""
                INSERT INTO {_qname(dest_schema, "metadata_artifact_manifest")}
                    ([hash_id], [target_table], [handler_version], [row_count])
                SELECT
                    d.[hash_id],
                    :target_table,
                    :handler_version,
                    COUNT(*) AS row_count
                FROM {_qname(dest_schema, table_name)} AS d
                GROUP BY d.hash_id
                """
            ),
            {"target_table": table_name, "handler_version": handler_version},
        )
        rows.append([table_name, "copy", result.rowcount or 0])

    return rows


def _prepare_environment(args: argparse.Namespace, dest_schema: str) -> None:
    if args.config:
        os.environ["E2UDE_CONFIG_PATH"] = str(Path(args.config).expanduser().resolve())
    os.environ["E2UDE_DATABASE__TYPE"] = "mssql"
    os.environ["E2UDE_DATABASE__SCHEMA_NAME"] = dest_schema


def _load_runtime():
    from e2ude_core.config import settings
    from e2ude_core.db.access import get_engine
    from e2ude_core.db.setup import _runtime_tables, initialize_database
    from e2ude_core.pipelines.scanner import SCANNER_VERSION
    from e2ude_core.runtime_files import CURRENT_HANDLER_GENERATION, HANDLED_FILE_SPECS

    table_versions = {
        model.__tablename__: spec.version
        for spec in HANDLED_FILE_SPECS
        for model in spec.expected_models
    }

    return (
        settings,
        get_engine,
        _runtime_tables,
        initialize_database,
        table_versions,
        SCANNER_VERSION,
        CURRENT_HANDLER_GENERATION,
    )


def _validate_source(
    conn: sa.Connection, source_schema: str, source_tables: set[str]
) -> None:
    if not _schema_exists(conn, source_schema):
        raise SystemExit(f"Source schema [{source_schema}] does not exist.")

    missing = sorted(REQUIRED_LEGACY_TABLES - source_tables)
    if missing:
        raise SystemExit(
            f"Source schema [{source_schema}] is missing required legacy tables: "
            f"{', '.join(missing)}"
        )

    invalid_md5 = _invalid_md5_count(conn, source_schema)
    if invalid_md5:
        raise SystemExit(
            f"Source metadata_hash_registry has {invalid_md5} invalid varchar MD5 values."
        )


def _print_plan(
    *,
    source_schema: str,
    dest_schema: str,
    source_counts: list[list[object]],
    file_counts: dict[str, int],
    mapping_counts: dict[str, dict[str, int]],
    selected_mapping: str,
    leaf_plan: list[list[object]],
    dest_exists: bool,
    dest_tables: set[str],
) -> None:
    print(f"Source      [{source_schema}]")
    print(f"Destination [{dest_schema}]")
    print("Source table counts")
    _print_table(["table", "rows"], source_counts)
    print("")
    print("File mapping")
    _print_table(
        [
            "method",
            "files",
            "missing_folder",
            "missing_archive",
            "ambiguous",
            "selected",
        ],
        [
            [
                method,
                counts["total_files"],
                counts["missing_folder"],
                counts["missing_archive"],
                counts["ambiguous_archive"],
                "yes" if method == selected_mapping else "",
            ]
            for method, counts in mapping_counts.items()
        ],
    )
    print(f"Selected mapping: {selected_mapping}")
    print(
        "Selected unmapped rows: "
        f"{file_counts['missing_folder'] + file_counts['missing_archive']}"
    )
    print("")
    print("Runtime leaf table plan")
    _print_table(["table", "action", "detail"], leaf_plan)
    print("")
    if dest_exists:
        print(f"Destination currently has {len(dest_tables)} table(s).")
    else:
        print("Destination schema does not exist yet.")


def seed_legacy_schema(args: argparse.Namespace) -> int:
    source_schema = validate_schema_name(args.source_schema)
    dest_schema = validate_schema_name(args.dest_schema)
    if source_schema == dest_schema:
        raise SystemExit("source_schema and dest_schema must be different.")
    if is_protected_schema(dest_schema):
        raise SystemExit(f"Refusing to write protected destination [{dest_schema}].")

    _prepare_environment(args, dest_schema)
    (
        settings,
        get_engine,
        runtime_tables_func,
        initialize_database,
        table_versions,
        scanner_version,
        handler_generation,
    ) = _load_runtime()
    if settings.database.type != "mssql":
        raise SystemExit("seed_legacy_schema.py only supports MSSQL.")

    eng = get_engine(settings.database)
    try:
        with eng.begin() as conn:
            source_tables = _schema_tables(conn, source_schema)
            _validate_source(conn, source_schema, source_tables)
            dest_exists = _schema_exists(conn, dest_schema)
            dest_tables = _schema_tables(conn, dest_schema) if dest_exists else set()
            if dest_tables and not args.replace:
                raise SystemExit(
                    f"Destination schema [{dest_schema}] is not empty. "
                    "Use --replace with --confirm-dest to rebuild it."
                )

            runtime_tables = runtime_tables_func()
            compatible_leaf_tables, leaf_plan = _table_compatibility(
                runtime_tables, conn, source_schema, source_tables
            )
            mapping_results = _folder_mapping_results(
                conn,
                source_schema,
                scanner_version=scanner_version,
                handler_generation=handler_generation,
            )
            mapping_counts = {
                method: result["counts"] for method, result in mapping_results.items()
            }
            selected_mapping = _select_folder_mapping(args.folder_map, mapping_counts)
            file_counts = mapping_counts[selected_mapping]
            selected_mapping_result = mapping_results[selected_mapping]
            source_counts = _source_table_counts(
                conn,
                source_schema,
                source_tables & (REQUIRED_LEGACY_TABLES | CONTROL_TABLES),
            )
            _print_plan(
                source_schema=source_schema,
                dest_schema=dest_schema,
                source_counts=source_counts,
                file_counts=file_counts,
                mapping_counts=mapping_counts,
                selected_mapping=selected_mapping,
                leaf_plan=leaf_plan,
                dest_exists=dest_exists,
                dest_tables=dest_tables,
            )

            if file_counts["ambiguous_archive"]:
                print("")
                print("Ambiguous file mapping examples")
                _print_table(
                    [
                        "file_id",
                        "folder_id",
                        "FolderPath",
                        "folder_key",
                        "archive_matches",
                    ],
                    selected_mapping_result["examples"],
                )
                raise SystemExit(
                    f"Refusing to seed with ambiguous {selected_mapping} mappings."
                )

            if file_counts["missing_folder"] or file_counts["missing_archive"]:
                print("")
                print("Unmapped file examples")
                _print_table(
                    [
                        "file_id",
                        "folder_id",
                        "FolderPath",
                        "folder_key",
                        "archive_matches",
                    ],
                    selected_mapping_result["examples"],
                )
                print("")
                print("Archive key examples")
                _print_table(
                    ["archive_id", "source_path", "archive_key"],
                    selected_mapping_result["archive_examples"],
                )
                if not args.allow_unmapped_files:
                    raise SystemExit(
                        "Refusing to seed with unmapped metadata_file rows. "
                        "Fix FolderPath/archive mapping or pass --allow-unmapped-files "
                        "to skip unmapped files."
                    )

            if not args.yes:
                print("")
                print("Plan only. No changes were made. Re-run with --yes to seed.")
                return 0

            if args.replace:
                require_exact_confirmation(
                    expected_schema=dest_schema,
                    provided_schema=args.confirm_dest,
                    flag_name="--confirm-dest",
                )
                if dest_tables:
                    _drop_schema_tables(conn, dest_schema, dest_tables)

        initialize_database(eng, reset_tables=False)

        copied_rows = []
        copied_leaf_table_names = set()
        table_by_name = {table.name: table for table in runtime_tables_func()}
        with eng.begin() as conn:
            copied_rows.append(
                (
                    [
                        "metadata_archive",
                        _copy_metadata_archive_from_folders(
                            conn,
                            dest_schema,
                            selected_mapping_result["archive_rows"],
                        ),
                    ]
                    if selected_mapping == "legacy-folder"
                    else [
                        "metadata_archive",
                        _copy_common_table(
                            conn,
                            source_schema=source_schema,
                            dest_schema=dest_schema,
                            table=table_by_name["metadata_archive"],
                        ),
                    ]
                )
            )
            if "metadata_discovery_directory" in source_tables:
                copied_rows.append(
                    [
                        "metadata_discovery_directory",
                        _copy_common_table(
                            conn,
                            source_schema=source_schema,
                            dest_schema=dest_schema,
                            table=table_by_name["metadata_discovery_directory"],
                        ),
                    ]
                )
            copied_rows.append(
                [
                    "metadata_hash_registry",
                    _copy_hash_registry(conn, source_schema, dest_schema),
                ]
            )
            copied_rows.append(
                [
                    "metadata_file",
                    _copy_metadata_file(
                        conn,
                        source_schema,
                        dest_schema,
                        selected_mapping_result["folder_archive_ids"],
                    ),
                ]
            )

            for table_name, table in sorted(compatible_leaf_tables.items()):
                rowcount = _copy_common_table(
                    conn,
                    source_schema=source_schema,
                    dest_schema=dest_schema,
                    table=table,
                )
                copied_leaf_table_names.add(table_name)
                copied_rows.append([table_name, rowcount])

            manifest_rows = _build_artifact_manifest(
                conn,
                dest_schema=dest_schema,
                copied_leaf_tables=copied_leaf_table_names,
                table_versions=table_versions,
            )

        print("")
        print("Copied rows")
        _print_table(["table", "rows"], copied_rows)
        print("")
        print("Artifact manifest")
        _print_table(["target_table", "action", "rows"], manifest_rows)
        print("")
        print(f"Seed complete. Validate with: uv run e2ude schema check {dest_schema}")
    finally:
        eng.dispose()

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Seed a current e2ude schema from a legacy prod schema without modifying "
            "the source schema."
        )
    )
    parser.add_argument("source_schema", help="Legacy source schema to read")
    parser.add_argument("dest_schema", help="Fresh current-schema destination to write")
    parser.add_argument("--config", type=Path, help="Path to e2ude_config.toml")
    parser.add_argument("--yes", action="store_true", help="Actually write destination")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Drop existing destination tables before seeding",
    )
    parser.add_argument(
        "--confirm-dest",
        help="Exact destination schema confirmation required with --replace",
    )
    parser.add_argument(
        "--allow-unmapped-files",
        action="store_true",
        help="Skip legacy metadata_file rows that cannot be mapped to metadata_archive",
    )
    parser.add_argument(
        "--folder-map",
        choices=["auto", "exact-path", "archive-name", "legacy-folder"],
        default="auto",
        help=(
            "How legacy metadata_folder rows map to metadata_archive. "
            "auto prefers a complete exact path match, then a unique archive filename "
            "match, then rows derived from metadata_folder."
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return seed_legacy_schema(args)


if __name__ == "__main__":
    raise SystemExit(main())
