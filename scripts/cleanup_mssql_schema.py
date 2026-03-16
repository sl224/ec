import argparse

import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.schema_safety import (
    format_target_banner,
    is_protected_schema,
    require_exact_confirmation,
    validate_schema_name,
)


def _fetch_schema_tables(conn, schema_name: str) -> list[str]:
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


def _schema_exists(conn, schema_name: str) -> bool:
    return (
        conn.execute(
            sa.text("SELECT 1 FROM sys.schemas WHERE name = :schema_name"),
            {"schema_name": schema_name},
        ).scalar_one_or_none()
        is not None
    )


def _drop_foreign_keys(conn, schema_name: str):
    rows = conn.execute(
        sa.text(
            """
            SELECT
                OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
                OBJECT_NAME(fk.parent_object_id) AS table_name,
                fk.name AS constraint_name
            FROM sys.foreign_keys AS fk
            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name
            """
        ),
        {"schema_name": schema_name},
    ).fetchall()

    for row in rows:
        conn.execute(
            sa.text(
                f"ALTER TABLE [{row.schema_name}].[{row.table_name}] DROP CONSTRAINT [{row.constraint_name}]"
            )
        )


def _drop_tables(conn, schema_name: str, table_names: list[str]):
    for table_name in table_names:
        conn.execute(sa.text(f"DROP TABLE [{schema_name}].[{table_name}]"))


def main():
    parser = argparse.ArgumentParser(
        description="Preview or drop an MSSQL schema used for manual local E2E runs."
    )
    parser.add_argument(
        "--schema-name",
        default=getattr(settings.database, "schema_name", None),
        help="Schema to inspect or drop. Defaults to the configured MSSQL schema.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="List the schema tables without dropping anything.",
    )
    parser.add_argument(
        "--keep-schema",
        action="store_true",
        help="Drop the tables but leave the empty schema in place.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required to actually drop objects.",
    )
    parser.add_argument(
        "--confirm-schema",
        help="Exact schema name confirmation required for destructive actions.",
    )
    args = parser.parse_args()

    if settings.database.type != "mssql":
        raise RuntimeError(
            "cleanup_mssql_schema.py only supports MSSQL configurations."
        )

    if not args.schema_name:
        raise ValueError("No schema name provided.")

    schema_name = validate_schema_name(args.schema_name)
    eng = get_engine(settings.database)

    try:
        with eng.begin() as conn:
            print(format_target_banner(settings.database, schema_name=schema_name))
            if is_protected_schema(schema_name):
                print(
                    "WARNING: This schema is protected by convention because it is shared for day-to-day work."
                )

            if not _schema_exists(conn, schema_name):
                print(f"Schema [{schema_name}] does not exist.")
                return

            table_names = _fetch_schema_tables(conn, schema_name)

            print(f"Schema [{schema_name}]")
            if table_names:
                for table_name in table_names:
                    print(f"  - {table_name}")
            else:
                print("  (no tables)")

            if args.preview:
                return

            if not args.yes:
                raise SystemExit(
                    "Refusing to drop schema contents without --yes. Use --preview to inspect first."
                )
            require_exact_confirmation(
                expected_schema=schema_name,
                provided_schema=args.confirm_schema,
                flag_name="--confirm-schema",
            )

            if table_names:
                _drop_foreign_keys(conn, schema_name)
                _drop_tables(conn, schema_name, table_names)

            if not args.keep_schema:
                conn.execute(sa.text(f"DROP SCHEMA [{schema_name}]"))

            print(
                f"Dropped {len(table_names)} tables from [{schema_name}]"
                + ("" if args.keep_schema else " and removed the schema")
                + "."
            )
    finally:
        eng.dispose()


if __name__ == "__main__":
    main()
