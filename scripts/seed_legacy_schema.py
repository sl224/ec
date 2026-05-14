from __future__ import annotations

import argparse
import os
from pathlib import Path

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


def _column_types(conn: sa.Connection, schema_name: str, table_name: str) -> dict[str, str]:
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
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in text_rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _drop_schema_tables(conn: sa.Connection, schema_name: str, table_names: set[str]) -> None:
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
    md5_type = _column_types(conn, source_schema, "metadata_hash_registry").get("md5", "")
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


def _file_mapping_counts(conn: sa.Connection, source_schema: str) -> dict[str, int]:
    row = conn.execute(
        sa.text(
            f"""
            SELECT
                COUNT(*) AS total_files,
                SUM(CASE WHEN f.FolderID IS NULL THEN 1 ELSE 0 END) AS missing_folder,
                SUM(CASE WHEN a.id IS NULL THEN 1 ELSE 0 END) AS missing_archive
            FROM {_qname(source_schema, "metadata_file")} AS mf
            LEFT JOIN {_qname(source_schema, "metadata_folder")} AS f
                ON f.FolderID = mf.folder_id
            LEFT JOIN {_qname(source_schema, "metadata_archive")} AS a
                ON a.source_path = f.FolderPath
            """
        )
    ).one()
    return {
        "total_files": row.total_files or 0,
        "missing_folder": row.missing_folder or 0,
        "missing_archive": row.missing_archive or 0,
    }


def _unmapped_file_examples(
    conn: sa.Connection, source_schema: str, limit: int = 20
) -> list[list[object]]:
    rows = conn.execute(
        sa.text(
            f"""
            SELECT TOP ({limit})
                mf.id AS file_id,
                mf.folder_id,
                f.FolderPath
            FROM {_qname(source_schema, "metadata_file")} AS mf
            LEFT JOIN {_qname(source_schema, "metadata_folder")} AS f
                ON f.FolderID = mf.folder_id
            LEFT JOIN {_qname(source_schema, "metadata_archive")} AS a
                ON a.source_path = f.FolderPath
            WHERE f.FolderID IS NULL OR a.id IS NULL
            ORDER BY mf.id
            """
        )
    ).fetchall()
    return [[row.file_id, row.folder_id, row.FolderPath] for row in rows]


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
    conn.execute(sa.text(f"SET IDENTITY_INSERT {_qname(schema_name, table_name)} {state}"))


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
    md5_type = _column_types(conn, source_schema, "metadata_hash_registry").get("md5", "")
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


def _copy_metadata_file(
    conn: sa.Connection, source_schema: str, dest_schema: str
) -> int:
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
                INNER JOIN {_qname(source_schema, "metadata_folder")} AS f
                    ON f.FolderID = mf.folder_id
                INNER JOIN {_qname(dest_schema, "metadata_archive")} AS da
                    ON da.source_path = f.FolderPath
                """
            )
        )
    finally:
        _set_identity_insert(conn, dest_schema, "metadata_file", False)
    return result.rowcount or 0


def _copy_artifact_manifest(
    conn: sa.Connection,
    *,
    source_schema: str,
    dest_schema: str,
    copied_leaf_tables: set[str],
) -> list[list[object]]:
    if "metadata_artifact_manifest" not in _schema_tables(conn, source_schema):
        return []

    rows = []
    source_targets = conn.execute(
        sa.text(
            f"""
            SELECT DISTINCT target_table
            FROM {_qname(source_schema, "metadata_artifact_manifest")}
            ORDER BY target_table
            """
        )
    ).fetchall()

    for row in source_targets:
        table_name = row.target_table
        if table_name not in copied_leaf_tables:
            rows.append([table_name, "skip", "target table not copied"])
            continue

        result = conn.execute(
            sa.text(
                f"""
                INSERT INTO {_qname(dest_schema, "metadata_artifact_manifest")}
                    ([hash_id], [target_table], [handler_version], [row_count])
                SELECT
                    m.[hash_id],
                    m.[target_table],
                    MAX(m.[handler_version]) AS handler_version,
                    COALESCE(MAX(c.row_count), 0) AS row_count
                FROM {_qname(source_schema, "metadata_artifact_manifest")} AS m
                INNER JOIN {_qname(dest_schema, "metadata_hash_registry")} AS h
                    ON h.id = m.hash_id
                LEFT JOIN (
                    SELECT hash_id, COUNT(*) AS row_count
                    FROM {_qname(dest_schema, table_name)}
                    GROUP BY hash_id
                ) AS c
                    ON c.hash_id = m.hash_id
                WHERE m.target_table = :target_table
                GROUP BY m.hash_id, m.target_table
                """
            ),
            {"target_table": table_name},
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

    return settings, get_engine, _runtime_tables, initialize_database


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
        ["metric", "count"],
        [
            ["metadata_file rows", file_counts["total_files"]],
            ["missing metadata_folder", file_counts["missing_folder"]],
            ["missing metadata_archive by FolderPath", file_counts["missing_archive"]],
        ],
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
    settings, get_engine, runtime_tables_func, initialize_database = _load_runtime()
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
            file_counts = _file_mapping_counts(conn, source_schema)
            source_counts = _source_table_counts(
                conn, source_schema, source_tables & (REQUIRED_LEGACY_TABLES | CONTROL_TABLES)
            )
            _print_plan(
                source_schema=source_schema,
                dest_schema=dest_schema,
                source_counts=source_counts,
                file_counts=file_counts,
                leaf_plan=leaf_plan,
                dest_exists=dest_exists,
                dest_tables=dest_tables,
            )

            if file_counts["missing_folder"] or file_counts["missing_archive"]:
                print("")
                print("Unmapped file examples")
                _print_table(
                    ["file_id", "folder_id", "FolderPath"],
                    _unmapped_file_examples(conn, source_schema),
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
                [
                    "metadata_archive",
                    _copy_common_table(
                        conn,
                        source_schema=source_schema,
                        dest_schema=dest_schema,
                        table=table_by_name["metadata_archive"],
                    ),
                ]
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
                ["metadata_file", _copy_metadata_file(conn, source_schema, dest_schema)]
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

            manifest_rows = _copy_artifact_manifest(
                conn,
                source_schema=source_schema,
                dest_schema=dest_schema,
                copied_leaf_tables=copied_leaf_table_names,
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return seed_legacy_schema(args)


if __name__ == "__main__":
    raise SystemExit(main())
