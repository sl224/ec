from __future__ import annotations

import sqlite3
import subprocess
import sys
from io import BytesIO
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
import pytest
import sqlalchemy as sa

from e2ude_core.db.access import get_engine
from e2ude_core.db.models import (
    ArtifactManifest,
    SegmentsData,
    TmptrData,
)
from e2ude_core.db.setup import initialize_database
from e2ude_core.pipelines.base import process_file
from e2ude_core.runtime_files import FileType, RuntimeFileSpec


SEGMENTS_LINE = (
    "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,"
    "01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,"
    "false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,"
    "false,0,0,,,"
)

MCDATA_LINES = "\n".join(
    [
        (
            "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,"
            "COMM,CI,,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,"
            ",,,,,,,,,False,True,,,False,,,,,,,,,,"
        ),
        (
            "2,RFC_DB:,,02/03/2025 01:09:02,SCS,28546,CONFIRMED,01:09:02,"
            "NOT_BIT,ConsecTru,1,TotTru,1,ConsecFal,0,TotFal,0,TotCnt,1,"
            "28546,NONE,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
        ),
        (
            "3,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,"
            ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
        ),
    ]
)

TMPTR_LINE = "AFMC,00250203,01:09:02.123,TMPTR,085F,029C"

ENGINE_LINE = "ENG_TIME,L,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,01:23:00,src"


def _write_multi_parser_archive(zip_path):
    prefix = zip_path.name.replace("_TransportRSM.fpkg.e2d.zip", "")
    raw_archive_name = f"{prefix}_RSM_RawArchive.zip"
    nested_buffer = BytesIO()
    with ZipFile(nested_buffer, "w") as nested_zip:
        nested_zip.writestr("RSM/TMPTR_LOG", TMPTR_LINE + "\n")

    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr(f"{prefix}_Segments", SEGMENTS_LINE + "\n")
        zip_file.writestr(f"{prefix}_Engine", ENGINE_LINE + "\n")
        zip_file.writestr(f"{prefix}_MCData", MCDATA_LINES + "\n")
        zip_file.writestr(raw_archive_name, nested_buffer.getvalue())


def _run_refresh(run_repo_command, sqlite_path, config_path):
    return run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "refresh",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
        ],
        {
            "E2UDE_RUNTIME__DISCOVERY_WORKERS": 2,
            "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
        },
    )


def _multi_parser_counts(sqlite_path):
    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM metadata_archive) AS archives,
                (SELECT COUNT(*) FROM metadata_file) AS files,
                (SELECT COUNT(DISTINCT content_hash) FROM metadata_file
                    WHERE content_hash IS NOT NULL) AS hashes,
                (SELECT COUNT(*) FROM metadata_artifact_manifest) AS artifacts,
                (SELECT COALESCE(SUM(row_count), 0) FROM metadata_artifact_manifest) AS manifest_rows,
                (SELECT COUNT(*) FROM processing_sessions) AS sessions,
                (SELECT COUNT(*) FROM processing_jobs WHERE status = 'ERROR') AS error_jobs,
                (SELECT COUNT(*) FROM rsmdata_segments) AS segment_rows,
                (SELECT COUNT(*) FROM rsmdata_engine_on_off) AS engine_rows,
                (SELECT COUNT(*) FROM rsmdata_tmptr) AS tmptr_rows,
                (SELECT COUNT(*) FROM rsmdata_mc_pfc_db) AS pfc_rows,
                (SELECT COUNT(*) FROM rsmdata_mc_rfc_db) AS rfc_rows,
                (SELECT COUNT(*) FROM rsmdata_mc_lcs_temp) AS lcs_rows,
                (SELECT locator_path FROM metadata_archive LIMIT 1) AS locator_path
            """
        ).fetchone()
        return dict(row)


def _sqlite_counts(sqlite_path):
    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM metadata_archive) AS archives,
                (SELECT COUNT(*) FROM metadata_file) AS files,
                (SELECT COUNT(DISTINCT content_hash) FROM metadata_file
                    WHERE content_hash IS NOT NULL) AS hashes,
                (SELECT COUNT(*) FROM metadata_artifact_manifest) AS artifacts,
                (SELECT COUNT(*) FROM metadata_artifact_manifest
                    WHERE target_table = 'rsmdata_segments') AS segment_artifacts,
                (SELECT COUNT(*) FROM rsmdata_segments) AS segment_rows,
                (SELECT COUNT(*) FROM processing_sessions) AS sessions,
                (SELECT COUNT(*) FROM processing_jobs) AS jobs,
                (SELECT COUNT(*) FROM processing_jobs
                    WHERE parser_id = 'segments') AS segment_jobs,
                (SELECT COUNT(*) FROM processing_jobs
                    WHERE status = 'ERROR') AS error_jobs
            """
        ).fetchone()
        return dict(row)


def test_refresh_lifecycle_covers_catalog_hash_parse_manifest_and_status(
    run_repo_command, write_app_config, tmp_path
):
    from e2ude_core.runtime_files import HANDLED_FILE_SPECS

    sqlite_path = tmp_path / "multi_parser_lifecycle.sqlite3"
    scan_root = tmp_path / "scan_root"
    staging_root = tmp_path / "staging"
    archive_dir = scan_root / "169871" / "2025" / "01"
    archive_dir.mkdir(parents=True)
    archive_path = archive_dir / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    _write_multi_parser_archive(archive_path)

    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": scan_root,
            "staging_root": staging_root,
        },
    )

    _run_refresh(run_repo_command, sqlite_path, config_path)
    counts = _multi_parser_counts(sqlite_path)

    expected_artifacts = sum(len(spec.expected_models) for spec in HANDLED_FILE_SPECS)
    assert counts["archives"] == 1
    assert counts["files"] == 4
    assert counts["hashes"] == 4
    assert counts["artifacts"] == expected_artifacts
    assert counts["manifest_rows"] == 6
    assert counts["sessions"] == 1
    assert counts["error_jobs"] == 0
    assert counts["segment_rows"] == 1
    assert counts["engine_rows"] == 1
    assert counts["tmptr_rows"] == 1
    assert counts["pfc_rows"] == 1
    assert counts["rfc_rows"] == 1
    assert counts["lcs_rows"] == 1

    status = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "status",
            "--sqlite",
            str(sqlite_path),
            "--config",
            str(config_path),
        ]
    )
    for parser_id in ("engine_on_off", "mcdata", "segments", "tmptr_log"):
        assert parser_id in status.stdout
    assert "missing/stale" in status.stdout
    assert "Traceback" not in status.stdout + status.stderr


def test_refresh_is_idempotent_after_archive_move(
    run_repo_command, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "archive_move.sqlite3"
    scan_root = tmp_path / "scan_root"
    staging_root = tmp_path / "staging"
    first_dir = scan_root / "169871" / "2025" / "01"
    second_dir = scan_root / "169871" / "2025" / "02"
    first_dir.mkdir(parents=True)
    second_dir.mkdir(parents=True)
    archive_path = first_dir / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    _write_multi_parser_archive(archive_path)

    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": scan_root,
            "staging_root": staging_root,
        },
    )

    _run_refresh(run_repo_command, sqlite_path, config_path)
    first_counts = _multi_parser_counts(sqlite_path)
    _run_refresh(run_repo_command, sqlite_path, config_path)
    second_counts = _multi_parser_counts(sqlite_path)

    moved_path = second_dir / archive_path.name
    archive_path.replace(moved_path)
    _run_refresh(run_repo_command, sqlite_path, config_path)
    moved_counts = _multi_parser_counts(sqlite_path)

    stable_keys = {
        "hashes",
        "artifacts",
        "manifest_rows",
        "error_jobs",
        "segment_rows",
        "engine_rows",
        "tmptr_rows",
        "pfc_rows",
        "rfc_rows",
        "lcs_rows",
    }
    assert {key: first_counts[key] for key in stable_keys} == {
        key: second_counts[key] for key in stable_keys
    }
    assert {key: first_counts[key] for key in stable_keys} == {
        key: moved_counts[key] for key in stable_keys
    }
    assert first_counts["sessions"] == 1
    assert second_counts["sessions"] == 1
    assert moved_counts["sessions"] == 2
    assert moved_counts["archives"] == first_counts["archives"] + 1
    assert moved_counts["files"] == first_counts["files"] * 2
    assert first_counts["locator_path"] == str(archive_path)
    with sqlite3.connect(sqlite_path) as conn:
        presence = dict(
            conn.execute("SELECT locator_path, is_present FROM metadata_archive")
        )
    assert presence == {str(archive_path): 0, str(moved_path): 1}


def test_refresh_is_idempotent_for_duplicate_content_hashes(
    run_repo_command, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "duplicate_hash_refresh.sqlite3"
    scan_root = tmp_path / "scan_root"
    staging_root = tmp_path / "staging"
    archive_dir = scan_root / "169871" / "2025" / "01"
    archive_dir.mkdir(parents=True)

    archive_a = archive_dir / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    archive_b = archive_dir / "169871_20250113_141337_001_TransportRSM.fpkg.e2d.zip"
    for archive_path, member_name in (
        (archive_a, "169871_20250113_141336_001_Segments"),
        (archive_b, "169871_20250113_141337_001_Segments"),
    ):
        with ZipFile(archive_path, "w") as zip_file:
            zip_file.writestr(member_name, SEGMENTS_LINE + "\n")

    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": scan_root,
            "staging_root": staging_root,
        },
    )
    env = {
        "E2UDE_RUNTIME__DISCOVERY_WORKERS": 2,
        "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
    }
    command = [
        sys.executable,
        "-m",
        "e2ude_core.cli",
        "refresh",
        "--sqlite",
        str(sqlite_path),
        "--config",
        str(config_path),
    ]

    run_repo_command(command, env)
    first_counts = _sqlite_counts(sqlite_path)
    run_repo_command(command, env)
    second_counts = _sqlite_counts(sqlite_path)

    assert first_counts == second_counts
    assert first_counts["archives"] == 2
    assert first_counts["files"] == 2
    assert first_counts["hashes"] == 1
    assert first_counts["segment_artifacts"] == 1
    assert first_counts["segment_rows"] == 1
    assert first_counts["segment_jobs"] == 1
    assert first_counts["error_jobs"] == 0


def test_refresh_returns_nonzero_for_duplicate_zip_members(
    run_repo_command, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "duplicate_members.sqlite3"
    scan_root = tmp_path / "scan_root"
    staging_root = tmp_path / "staging"
    scan_root.mkdir(parents=True)

    archive_path = scan_root / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    member_name = "169871_20250113_141336_001_Engine"
    with ZipFile(archive_path, "w") as zip_file:
        zip_file.writestr(
            member_name,
            "ENG_TIME,L,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,01:23:00,src\n",
        )
        zip_file.writestr(
            member_name,
            "ENG_TIME,R,01/13/2025 16:13:36:825,01/13/2025 17:36:51:825,01:23:00,src\n",
        )

    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": scan_root,
            "staging_root": staging_root,
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
            ],
            {
                "E2UDE_RUNTIME__DISCOVERY_WORKERS": 2,
                "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
            },
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "Archive contains duplicate member paths" in output
    assert "UNIQUE constraint failed" not in output


def test_parser_backfill_missing_member_records_error_without_artifact(
    run_repo_command, run_repo_python, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "missing_member.sqlite3"
    staging_root = tmp_path / "staging"
    zip_path = tmp_path / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    missing_member = "169871_20250113_141336_001_Segments"
    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr("169871_20250113_141336_001_Status.txt", "status\n")

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
    run_repo_python(
        """
import hashlib
import os
from datetime import datetime
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ArchiveMetadata, FileMetadata
from e2ude_core.db.setup import initialize_database
from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION

zip_path = Path(os.environ["ZIP_PATH"])
missing_member = os.environ["MISSING_MEMBER"]
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
    conn.execute(
        sa.insert(FileMetadata).values(
            archive_id=archive_id,
            content_hash=hashlib.md5(b"missing member content").digest(),
            relative_path=missing_member,
            file_size_bytes=123,
        )
    )
""",
        {
            "E2UDE_CONFIG_PATH": config_path,
            "ZIP_PATH": zip_path,
            "MISSING_MEMBER": missing_member,
        },
    )

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        run_repo_command(
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
            ]
        )

    output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
    assert "FAILED archive=" in output
    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM metadata_artifact_manifest) AS artifacts,
                (SELECT COUNT(*) FROM rsmdata_segments) AS segment_rows,
                (SELECT COUNT(*) FROM processing_sessions
                    WHERE status = 'ERROR') AS error_sessions,
                    (SELECT COUNT(*) FROM processing_jobs
                        WHERE status = 'ERROR'
                          AND parser_id = 'segments'
                          AND target_table IS NULL) AS error_jobs
            """
        ).fetchone()

    assert dict(row) == {
        "artifacts": 0,
        "segment_rows": 0,
        "error_sessions": 1,
        "error_jobs": 1,
    }


def test_retry_failed_includes_failed_content_hash_jobs(
    run_repo_command, run_repo_python, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "retry_hash_failure.sqlite3"
    staging_root = tmp_path / "staging"
    zip_path = tmp_path / "169871_20250113_141336_001_TransportRSM.fpkg.e2d.zip"
    missing_member = "169871_20250113_141336_001_Engine"
    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr("169871_20250113_141336_001_Status.txt", "status\n")

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
    run_repo_python(
        """
import os
from datetime import datetime
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ArchiveMetadata, FileMetadata
from e2ude_core.db.setup import initialize_database
from e2ude_core.runtime_files import CURRENT_ARCHIVE_CATALOG_VERSION

zip_path = Path(os.environ["ZIP_PATH"])
missing_member = os.environ["MISSING_MEMBER"]
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
    conn.execute(
        sa.insert(FileMetadata).values(
            archive_id=archive_id,
            content_hash=None,
            relative_path=missing_member,
            file_size_bytes=123,
        )
    )
""",
        {
            "E2UDE_CONFIG_PATH": config_path,
            "ZIP_PATH": zip_path,
            "MISSING_MEMBER": missing_member,
        },
    )

    with pytest.raises(subprocess.CalledProcessError):
        run_repo_command(
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

    retry_plan = run_repo_command(
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
            "--plan",
        ]
    )
    assert "Pending     1" in retry_plan.stdout


def test_process_file_rolls_back_all_tables_when_one_output_fails(tmp_path):
    db_path = tmp_path / "parser_atomicity.sqlite3"
    eng = get_engine(
        type(
            "DbSettings",
            (),
            {"type": "sqlite3", "db_location": db_path, "in_memory": False},
        )()
    )
    initialize_database(eng, reset_tables=True)
    source_file = tmp_path / "input.txt"
    source_file.write_text("ignored\n", encoding="utf-8")

    content_hash = b"\x01" * 16

    def parse_bad_output(_path):
        return {
            SegmentsData: pd.DataFrame(
                [
                    {
                        "line_number": 1,
                        "group": 1,
                        "event_start": datetime(2025, 1, 13, 14, 13, 36),
                        "event_stop": datetime(2025, 1, 13, 15, 36, 51),
                        "flight_status": "PreFlight",
                    }
                ]
            ),
            TmptrData: pd.DataFrame(
                [
                    {
                        "line_number": 1,
                        "afmc": "AFMC",
                        "datetime": datetime(2025, 2, 3, 1, 9, 2),
                    },
                    {
                        "line_number": 1,
                        "afmc": "AFMC",
                        "datetime": datetime(2025, 2, 3, 1, 9, 3),
                    },
                ]
            ),
        }

    spec = RuntimeFileSpec(
        FileType.SEGMENTS,
        ("*",),
        "atomicity_test",
        1,
        parse_bad_output,
        (SegmentsData, TmptrData),
    )

    with pytest.raises(Exception):
        process_file(
            eng=eng,
            spec=spec,
            content_hash=content_hash,
            file_path=source_file,
            report_progress=lambda _message: None,
        )

    with eng.connect() as conn:
        assert (
            conn.execute(
                sa.select(sa.func.count()).select_from(SegmentsData)
            ).scalar_one()
            == 0
        )
        assert (
            conn.execute(sa.select(sa.func.count()).select_from(TmptrData)).scalar_one()
            == 0
        )
        assert (
            conn.execute(
                sa.select(sa.func.count()).select_from(ArtifactManifest)
            ).scalar_one()
            == 0
        )
