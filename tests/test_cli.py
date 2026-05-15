from __future__ import annotations

import json
import subprocess
import sys
from zipfile import ZipFile

import pytest


SEGMENTS_LINE = (
    "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,"
    "01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,"
    "false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,"
    "false,0,0,,,"
)


def test_cli_help_teaches_common_user_flows(run_repo_command):
    result = run_repo_command([sys.executable, "-m", "e2ude_core.cli", "--help"])

    assert "Backfill one parser" in result.stdout
    assert "cataloged history" in result.stdout
    assert "shared dev SQL Server schema" in result.stdout
    assert "local SQLite database" in result.stdout
    assert "parser" in result.stdout
    assert "schema" in result.stdout
    assert "artifacts" not in result.stdout


def test_parser_help_focuses_parser_workflows(run_repo_command):
    result = run_repo_command(
        [sys.executable, "-m", "e2ude_core.cli", "parser", "--help"]
    )

    assert "writing a parser" in result.stdout
    assert "backfill" in result.stdout
    assert "invalidate" in result.stdout
    assert "preview" in result.stdout


def test_parser_backfill_help_explains_history_and_from_file(run_repo_command):
    result = run_repo_command(
        [sys.executable, "-m", "e2ude_core.cli", "parser", "backfill", "--help"]
    )

    assert "new parser" in result.stdout
    assert "historical matching files" in result.stdout
    assert "cataloged archive files" in result.stdout
    assert "does not discover new archives" in result.stdout
    assert "does not parse that local file" in result.stdout
    assert "--plan" in result.stdout


def test_parser_invalidate_help_is_available(run_repo_command):
    result = run_repo_command(
        [sys.executable, "-m", "e2ude_core.cli", "parser", "invalidate", "--help"]
    )

    assert "Delete manifest rows for one parser" in result.stdout
    assert "--yes" in result.stdout


def test_cli_refresh_preview_prints_explicit_targets(
    run_repo_command, write_app_config, tmp_path
):
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": "TESTSERVER",
            "db_name": "TESTDB",
            "driver": "ODBC Driver 17 for SQL Server",
            "trusted_connection": "yes",
            "schema_name": "from_config",
        },
        paths={
            "scan_root": tmp_path / "scan_root",
            "staging_root": tmp_path / "staging",
        },
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "refresh",
            "--schema",
            "e2ude_candidate_cli",
            "--config",
            str(config_path),
            "--preview",
        ]
    )

    assert "backend   mssql" in result.stdout
    assert "server    TESTSERVER" in result.stdout
    assert "database  TESTDB" in result.stdout
    assert "schema    e2ude_candidate_cli" in result.stdout
    assert "command   refresh" in result.stdout

    sqlite_path = tmp_path / "local.sqlite3"
    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "refresh",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--preview",
        ]
    )

    assert "backend   sqlite" in result.stdout
    assert f"file      {sqlite_path}" in result.stdout
    assert "schema    n/a" in result.stdout


def test_cli_refresh_rejects_unsafe_schema(
    run_repo_command, write_app_config, tmp_path
):
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": "TESTSERVER",
            "db_name": "TESTDB",
            "schema_name": "from_config",
        }
    )

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_repo_command(
            [
                sys.executable,
                "-m",
                "e2ude_core.cli",
                "refresh",
                "--schema",
                "bad-name",
                "--config",
                str(config_path),
                "--preview",
            ]
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "unsafe schema name" in output


def test_cli_refresh_env_selection_and_prod_confirmation(
    run_repo_command, write_app_config, tmp_path
):
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": "TESTSERVER",
            "db_name": "TESTDB",
            "schema_name": "from_config",
        }
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "refresh",
            "--env",
            "dev",
            "--config",
            str(config_path),
            "--preview",
        ]
    )
    assert "schema    e2ude_core_dev" in result.stdout

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_repo_command(
            [
                sys.executable,
                "-m",
                "e2ude_core.cli",
                "refresh",
                "--env",
                "prod",
                "--config",
                str(config_path),
            ]
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "Refusing destructive action without --confirm 'e2ude_core'" in output


def test_cli_lists_registered_parsers(run_repo_command):
    from e2ude_core.runtime_files import HANDLED_FILE_SPECS

    result = run_repo_command(
        [sys.executable, "-m", "e2ude_core.cli", "parser", "list"]
    )

    for spec in HANDLED_FILE_SPECS:
        assert spec.parser_id in result.stdout
        assert spec.file_type.value in result.stdout


def test_cli_preview_supports_local_hint_and_explicit_parser(
    run_repo_command, tmp_path
):
    tmptr_file = tmp_path / "TMPTR_LOG"
    tmptr_file.write_text(
        "AFMC,00250203,01:09:02.123,TMPTR,085F,029C\n",
        encoding="utf-8",
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "preview",
            str(tmptr_file),
            "--head",
            "1",
        ]
    )
    payload = json.loads(result.stdout)

    assert payload["selected_parser"] == "tmptr_log"
    assert payload["selection_source"] == "local filename"
    assert payload["tables"][0]["table"] == "rsmdata_tmptr"
    assert payload["tables"][0]["rows"] == 1

    weird_segments = tmp_path / "developer_sample.txt"
    weird_segments.write_text(SEGMENTS_LINE + "\n", encoding="utf-8")

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "preview",
            str(weird_segments),
            "--as",
            "segments",
            "--head",
            "1",
        ]
    )
    payload = json.loads(result.stdout)

    assert payload["selected_file_type"] == "SEGMENTS"
    assert payload["selection_source"] == "explicit"
    assert payload["tables"][0]["table"] == "rsmdata_segments"
    assert payload["tables"][0]["preview"][0]["flight_status"] == "PreFlight"


def test_cli_preview_unknown_local_file_suggests_parser_choices(
    run_repo_command, tmp_path
):
    sample_file = tmp_path / "developer_sample.dat"
    sample_file.write_text("not enough naming context\n", encoding="utf-8")

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_repo_command(
            [
                sys.executable,
                "-m",
                "e2ude_core.cli",
                "parser",
                "preview",
                str(sample_file),
            ]
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "Could not infer parser" in output
    assert "--as engine_on_off" in output
    assert "--as segments" in output
    assert "Traceback" not in output


def test_cli_backfill_processes_only_selected_parser_artifacts(
    run_repo_command, run_repo_python, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "parser_backfill.sqlite3"
    staging_root = tmp_path / "staging"
    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": tmp_path / "scan_root",
            "staging_root": staging_root,
        },
    )

    zip_path = tmp_path / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    relative_path = "169871_20250113_141336_001_Segments"
    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr(relative_path, SEGMENTS_LINE + "\n")

    setup = run_repo_python(
        """
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import (
    ArchiveMetadata,
    FileMetadata,
)
from e2ude_core.db.setup import initialize_database
from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION

zip_path = Path(os.environ["TEST_ZIP"])
relative_path = os.environ["TEST_RELATIVE_PATH"]
member_bytes = ZipFile(zip_path).read(relative_path)
stat = zip_path.stat()

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
with eng.begin() as conn:
    archive_id = conn.execute(
        sa.insert(ArchiveMetadata)
        .values(
            archive_key="169871_20250113_141336_001",
            buno="169871",
            archive_datetime=datetime(2025, 1, 13, 14, 13, 36),
            locator_key=str(zip_path).casefold(),
            locator_path=str(zip_path),
            locator_size_bytes=stat.st_size,
            locator_mtime_ns=stat.st_mtime_ns,
            cataloged_at=sa.func.now(),
            catalog_version=CURRENT_ARCHIVE_CATALOG_VERSION,
        )
        .returning(ArchiveMetadata.id)
    ).scalar_one()
    content_hash = hashlib.md5(member_bytes).digest()
    file_id = conn.execute(
        sa.insert(FileMetadata)
        .values(
            archive_id=archive_id,
            content_hash=content_hash,
            relative_path=relative_path,
            file_size_bytes=len(member_bytes),
        )
        .returning(FileMetadata.id)
    ).scalar_one()

print(
    json.dumps(
        {
            "archive_id": archive_id,
            "file_id": file_id,
            "content_hash": content_hash.hex(),
        }
    )
)
""",
        {
            "E2UDE_CONFIG_PATH": config_path,
            "TEST_ZIP": zip_path,
            "TEST_RELATIVE_PATH": relative_path,
        },
    )
    ids = json.loads(setup.stdout)

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--plan",
        ]
    )
    assert "Parser      segments" in result.stdout
    assert "Pending     1" in result.stdout
    assert str(ids["file_id"]) in result.stdout

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "mcdata",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--plan",
        ]
    )
    assert "Parser      mcdata" in result.stdout
    assert "Pending     0" in result.stdout

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--staging-root",
            str(staging_root),
            "--limit",
            "1",
        ]
    )
    payload = json.loads(result.stdout[result.stdout.rfind("{") :])
    assert payload == {
        "parser": "segments",
        "processed": 1,
        "failed": 0,
        "rows_uploaded": 1,
    }

    audit = run_repo_python(
        """
import json
import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

eng = get_engine(settings.database)
with eng.connect() as conn:
    row = conn.execute(
        sa.text(
            '''
            SELECT
                COUNT(DISTINCT s.id) AS sessions,
                COUNT(DISTINCT j.id) AS jobs,
                MAX(j.status) AS job_status,
                MAX(j.rows_uploaded) AS rows_uploaded,
                COUNT(DISTINCT m.target_table) AS artifacts,
                (SELECT COUNT(*) FROM rsmdata_segments) AS segment_rows
            FROM metadata_archive AS a
            LEFT JOIN processing_jobs AS j ON j.archive_id = a.id
            LEFT JOIN processing_sessions AS s ON s.id = j.session_id
            LEFT JOIN metadata_artifact_manifest AS m ON m.content_hash = j.content_hash
            GROUP BY a.id
            '''
        )
    ).one()

print(
    json.dumps(
        {
            "sessions": row.sessions,
            "jobs": row.jobs,
            "job_status": row.job_status,
            "rows_uploaded": row.rows_uploaded,
            "artifacts": row.artifacts,
            "segment_rows": row.segment_rows,
        }
    )
)
""",
        {"E2UDE_CONFIG_PATH": config_path},
    )
    payload = json.loads(audit.stdout)

    assert payload["sessions"] == 1
    assert payload["jobs"] == 1
    assert payload["job_status"] == "COMPLETED"
    assert payload["rows_uploaded"] == 1
    assert payload["artifacts"] == 1
    assert payload["segment_rows"] == 1

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "list",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--counts",
        ]
    )
    assert "segments" in result.stdout
    assert "missing/stale" in result.stdout
    assert "rows" in result.stdout

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "status",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
        ]
    )
    assert "segments" in result.stdout
    assert "complete" in result.stdout
    assert "missing/stale" in result.stdout

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--force",
            "--plan",
        ]
    )
    assert "Pending     1" in result.stdout

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "invalidate",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--plan",
        ]
    )
    assert "Manifest rows  1" in result.stdout

    run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "invalidate",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--yes",
        ]
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--plan",
        ]
    )
    assert "Pending     1" in result.stdout


def test_cli_backfill_records_zero_row_artifacts(
    run_repo_command, run_repo_python, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "zero_row_artifacts.sqlite3"
    staging_root = tmp_path / "staging"
    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": tmp_path / "scan_root",
            "staging_root": staging_root,
        },
    )

    zip_path = tmp_path / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    relative_path = "169871_20250113_141336_001_Engine"
    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr(
            relative_path,
            "OTHER,1,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,01:23:00,src\n",
        )

    run_repo_python(
        """
import hashlib
import os
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import (
    ArchiveMetadata,
    FileMetadata,
)
from e2ude_core.db.setup import initialize_database
from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION

zip_path = Path(os.environ["TEST_ZIP"])
relative_path = os.environ["TEST_RELATIVE_PATH"]
member_bytes = ZipFile(zip_path).read(relative_path)
stat = zip_path.stat()

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
with eng.begin() as conn:
    archive_id = conn.execute(
        sa.insert(ArchiveMetadata)
        .values(
            archive_key="169871_20250113_141336_001",
            buno="169871",
            archive_datetime=datetime(2025, 1, 13, 14, 13, 36),
            locator_key=str(zip_path).casefold(),
            locator_path=str(zip_path),
            locator_size_bytes=stat.st_size,
            locator_mtime_ns=stat.st_mtime_ns,
            cataloged_at=sa.func.now(),
            catalog_version=CURRENT_ARCHIVE_CATALOG_VERSION,
        )
        .returning(ArchiveMetadata.id)
    ).scalar_one()
    content_hash = hashlib.md5(member_bytes).digest()
    conn.execute(
        sa.insert(FileMetadata).values(
            archive_id=archive_id,
            content_hash=content_hash,
            relative_path=relative_path,
            file_size_bytes=len(member_bytes),
        )
    )
""",
        {
            "E2UDE_CONFIG_PATH": config_path,
            "TEST_ZIP": zip_path,
            "TEST_RELATIVE_PATH": relative_path,
        },
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "engine_on_off",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--staging-root",
            str(staging_root),
        ]
    )
    payload = json.loads(result.stdout[result.stdout.rfind("{") :])
    assert payload == {
        "parser": "engine_on_off",
        "processed": 1,
        "failed": 0,
        "rows_uploaded": 0,
    }

    audit = run_repo_python(
        """
import json
import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

eng = get_engine(settings.database)
with eng.connect() as conn:
    row = conn.execute(
        sa.text(
            '''
            SELECT artifact_key, target_table, parser_version, row_count
            FROM metadata_artifact_manifest
            WHERE artifact_key = 'engine_on_off'
            '''
        )
    ).one()
    engine_rows = conn.execute(sa.text("SELECT COUNT(*) FROM rsmdata_engine_on_off")).scalar_one()

print(
    json.dumps(
        {
            "artifact_key": row.artifact_key,
            "target_table": row.target_table,
            "parser_version": row.parser_version,
            "row_count": row.row_count,
            "engine_rows": engine_rows,
        }
    )
)
""",
        {"E2UDE_CONFIG_PATH": config_path},
    )
    payload = json.loads(audit.stdout)

    assert payload == {
        "artifact_key": "engine_on_off",
        "target_table": "rsmdata_engine_on_off",
        "parser_version": 2,
        "row_count": 0,
        "engine_rows": 0,
    }


def test_parser_commands_on_empty_sqlite_do_not_traceback(run_repo_command, tmp_path):
    sqlite_path = tmp_path / "empty.sqlite3"

    list_result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "list",
            "--counts",
            "--sqlite",
            str(sqlite_path),
        ]
    )
    assert "engine_on_off" in list_result.stdout
    assert "Traceback" not in list_result.stdout + list_result.stderr

    status_result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "status",
            "engine_on_off",
            "--sqlite",
            str(sqlite_path),
        ]
    )
    assert "engine_on_off" in status_result.stdout
    assert "Traceback" not in status_result.stdout + status_result.stderr

    backfill_result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "backfill",
            "engine_on_off",
            "--sqlite",
            str(sqlite_path),
            "--plan",
        ]
    )
    assert "Run refresh first" in backfill_result.stdout
    assert "Traceback" not in backfill_result.stdout + backfill_result.stderr


def test_refresh_missing_scan_root_returns_nonzero(
    run_repo_command, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "missing_root.sqlite3"
    missing_scan_root = tmp_path / "missing_scan_root"
    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": missing_scan_root,
            "staging_root": tmp_path / "staging",
        },
    )

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_repo_command(
            [
                sys.executable,
                "-m",
                "e2ude_core.cli",
                "refresh",
                "--sqlite",
                str(sqlite_path),
                "--config",
                str(config_path),
            ]
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "Scan root not found" in output
