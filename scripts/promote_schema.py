import argparse
from datetime import datetime

import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.schema_safety import (
    format_target_banner,
    is_protected_schema,
    require_exact_confirmation,
    validate_schema_name,
)


def _schema_exists(conn, schema_name: str) -> bool:
    return (
        conn.execute(
            sa.text("SELECT 1 FROM sys.schemas WHERE name = :schema_name"),
            {"schema_name": schema_name},
        ).scalar_one_or_none()
        is not None
    )


def _ensure_schema(conn, schema_name: str):
    if not _schema_exists(conn, schema_name):
        conn.execute(sa.text(f"CREATE SCHEMA [{schema_name}]"))


def _fetch_tables(conn, schema_name: str) -> list[str]:
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


def _print_schema_tables(label: str, table_names: list[str]):
    print(f"{label}:")
    if not table_names:
        print("  (no tables)")
        return
    for table_name in table_names:
        print(f"  - {table_name}")


def _transfer_tables(
    conn, source_schema: str, target_schema: str, table_names: list[str]
):
    for table_name in table_names:
        conn.execute(
            sa.text(
                f"ALTER SCHEMA [{target_schema}] TRANSFER [{source_schema}].[{table_name}]"
            )
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Promote a versioned MSSQL candidate schema into a stable target schema."
        )
    )
    parser.add_argument(
        "--source-schema", required=True, help="Candidate schema to promote"
    )
    parser.add_argument(
        "--target-schema",
        default="e2ude_core",
        help="Stable consumer-facing schema to receive the promoted tables",
    )
    parser.add_argument(
        "--archive-schema",
        help=(
            "Schema to receive the current target tables. Defaults to a timestamped "
            "backup schema derived from the target name."
        ),
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Show the planned transfers without modifying the database.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required to perform the promotion.",
    )
    parser.add_argument(
        "--confirm-target-schema",
        help="Required when promoting into a protected shared schema.",
    )
    args = parser.parse_args()

    if settings.database.type != "mssql":
        raise RuntimeError("promote_schema.py only supports MSSQL configurations.")

    source_schema = validate_schema_name(args.source_schema)
    target_schema = validate_schema_name(args.target_schema)
    archive_schema = validate_schema_name(
        args.archive_schema
        or f"{target_schema}_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    )

    if source_schema == target_schema:
        raise ValueError("source-schema and target-schema must be different.")
    if archive_schema in {source_schema, target_schema}:
        raise ValueError(
            "archive-schema must be distinct from source and target schemas."
        )
    if is_protected_schema(source_schema):
        raise ValueError(
            f"Protected shared schema [{source_schema}] cannot be used as a promotion source."
        )
    if is_protected_schema(archive_schema):
        raise ValueError(
            f"Protected shared schema [{archive_schema}] cannot be used as an archive target."
        )

    eng = get_engine(settings.database)

    try:
        with eng.begin() as conn:
            print(format_target_banner(settings.database, schema_name=source_schema))
            print(format_target_banner(settings.database, schema_name=target_schema))
            print(format_target_banner(settings.database, schema_name=archive_schema))

            if not _schema_exists(conn, source_schema):
                raise ValueError(f"Source schema [{source_schema}] does not exist.")

            source_tables = _fetch_tables(conn, source_schema)
            if not source_tables:
                raise ValueError(
                    f"Source schema [{source_schema}] has no tables to promote."
                )

            target_tables = (
                _fetch_tables(conn, target_schema)
                if _schema_exists(conn, target_schema)
                else []
            )
            archive_tables = (
                _fetch_tables(conn, archive_schema)
                if _schema_exists(conn, archive_schema)
                else []
            )

            _print_schema_tables(f"Source [{source_schema}]", source_tables)
            _print_schema_tables(f"Target [{target_schema}]", target_tables)
            _print_schema_tables(f"Archive [{archive_schema}]", archive_tables)

            if archive_tables:
                raise ValueError(
                    f"Archive schema [{archive_schema}] is not empty. Use a fresh archive schema."
                )

            print("")
            print(f"Planned promotion: [{source_schema}] -> [{target_schema}]")
            if target_tables:
                print(f"Existing target tables will be archived to [{archive_schema}]")

            if args.preview:
                return

            if not args.yes:
                raise SystemExit(
                    "Refusing to promote without --yes. Use --preview to inspect the plan first."
                )
            if is_protected_schema(target_schema):
                require_exact_confirmation(
                    expected_schema=target_schema,
                    provided_schema=args.confirm_target_schema,
                    flag_name="--confirm-target-schema",
                )

            _ensure_schema(conn, target_schema)
            _ensure_schema(conn, archive_schema)

            if target_tables:
                _transfer_tables(conn, target_schema, archive_schema, target_tables)

            _transfer_tables(conn, source_schema, target_schema, source_tables)

            print(
                f"Promoted {len(source_tables)} tables from [{source_schema}] to [{target_schema}] "
                f"and archived {len(target_tables)} tables to [{archive_schema}]."
            )
    finally:
        eng.dispose()


if __name__ == "__main__":
    main()
