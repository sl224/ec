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
            "--target",
            "mssql-dev",
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
                "--target",
                "mssql-dev",
                "--schema",
                "bad-name",
                "--config",
                str(config_path),
                "--preview",
            ]
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "unsafe schema name" in output


def test_cli_refresh_env_aliases_and_prod_confirmation(
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
            "--dry-run",
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

    result = run_repo_command([sys.executable, "-m", "e2ude_core.cli", "parsers"])

    for spec in HANDLED_FILE_SPECS:
        assert spec.pipeline_id.value in result.stdout
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
    FileHashRegistry,
    FileMetadata,
)
from e2ude_core.db.setup import initialize_database
from e2ude_core.pipelines.scanner import SCANNER_VERSION

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
            buno="169871",
            archive_datetime=datetime(2025, 1, 13, 14, 13, 36),
            source_path=str(zip_path),
            source_size_bytes=stat.st_size,
            source_mtime_ns=stat.st_mtime_ns,
            required_scan_version=SCANNER_VERSION,
            completed_scan_version=SCANNER_VERSION,
        )
        .returning(ArchiveMetadata.id)
    ).scalar_one()
    hash_id = conn.execute(
        sa.insert(FileHashRegistry)
        .values(md5=hashlib.md5(member_bytes).digest())
        .returning(FileHashRegistry.id)
    ).scalar_one()
    file_id = conn.execute(
        sa.insert(FileMetadata)
        .values(
            archive_id=archive_id,
            hash_id=hash_id,
            relative_path=relative_path,
            file_type="SEGMENTS",
            file_size_bytes=len(member_bytes),
        )
        .returning(FileMetadata.id)
    ).scalar_one()

print(json.dumps({"archive_id": archive_id, "file_id": file_id, "hash_id": hash_id}))
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

    run_repo_python(
        """
import os
import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob, ProcessingSession, StatusEnum

eng = get_engine(settings.database)
with eng.begin() as conn:
    session_id = conn.execute(
        sa.insert(ProcessingSession)
        .values(status=StatusEnum.ERROR)
        .returning(ProcessingSession.id)
    ).scalar_one()
    conn.execute(
        sa.insert(ProcessingJob).values(
            session_id=session_id,
            archive_id=int(os.environ["ARCHIVE_ID"]),
            file_type="SEGMENTS",
            parser_id="segments",
            target_table="rsmdata_segments",
            parser_version=1,
            rows_uploaded=0,
            status=StatusEnum.ERROR,
            message="test failure",
            file_id=int(os.environ["FILE_ID"]),
            hash_id=int(os.environ["HASH_ID"]),
        )
    )
""",
        {
            "E2UDE_CONFIG_PATH": config_path,
            "ARCHIVE_ID": ids["archive_id"],
            "FILE_ID": ids["file_id"],
            "HASH_ID": ids["hash_id"],
        },
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "retry-failed",
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

    run_repo_python(
        """
import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob, ProcessingSession

eng = get_engine(settings.database)
with eng.begin() as conn:
    conn.execute(sa.delete(ProcessingJob))
    conn.execute(sa.delete(ProcessingSession))
""",
        {"E2UDE_CONFIG_PATH": config_path},
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
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
            LEFT JOIN metadata_artifact_manifest AS m ON m.hash_id = j.hash_id
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
            "parsers",
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
            "--dry-run",
        ]
    )
    assert "Pending     1" in result.stdout

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "artifacts",
            "invalidate",
            "segments",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
            "--dry-run",
        ]
    )
    assert "Artifacts   1" in result.stdout

    run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "artifacts",
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
            "--dry-run",
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
    FileHashRegistry,
    FileMetadata,
)
from e2ude_core.db.setup import initialize_database
from e2ude_core.pipelines.scanner import SCANNER_VERSION

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
            buno="169871",
            archive_datetime=datetime(2025, 1, 13, 14, 13, 36),
            source_path=str(zip_path),
            source_size_bytes=stat.st_size,
            source_mtime_ns=stat.st_mtime_ns,
            required_scan_version=SCANNER_VERSION,
            completed_scan_version=SCANNER_VERSION,
        )
        .returning(ArchiveMetadata.id)
    ).scalar_one()
    hash_id = conn.execute(
        sa.insert(FileHashRegistry)
        .values(md5=hashlib.md5(member_bytes).digest())
        .returning(FileHashRegistry.id)
    ).scalar_one()
    conn.execute(
        sa.insert(FileMetadata).values(
            archive_id=archive_id,
            hash_id=hash_id,
            relative_path=relative_path,
            file_type="ENGINE_ON_OFF",
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
            SELECT target_table, parser_version, row_count
            FROM metadata_artifact_manifest
            WHERE target_table = 'rsmdata_engine_on_off5'
            '''
        )
    ).one()
    engine_rows = conn.execute(sa.text("SELECT COUNT(*) FROM rsmdata_engine_on_off5")).scalar_one()

print(
    json.dumps(
        {
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
        "target_table": "rsmdata_engine_on_off5",
        "parser_version": 1,
        "row_count": 0,
        "engine_rows": 0,
    }
