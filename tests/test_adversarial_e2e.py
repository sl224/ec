from __future__ import annotations

import sqlite3
import subprocess
import sys
from datetime import datetime
from zipfile import ZipFile

import pandas as pd
import pytest
import sqlalchemy as sa

from e2ude_core.db.access import get_engine
from e2ude_core.db.models import (
    ArtifactManifest,
    FileHashRegistry,
    SegmentsData,
    TmptrData,
)
from e2ude_core.db.setup import initialize_database
from e2ude_core.pipelines.base import process_file
from e2ude_core.runtime_files import FileType, PipelineId, RuntimeFileSpec


SEGMENTS_LINE = (
    "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,"
    "01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,"
    "false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,"
    "false,0,0,,,"
)


def _sqlite_counts(sqlite_path):
    with sqlite3.connect(sqlite_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM metadata_archive) AS archives,
                (SELECT COUNT(*) FROM metadata_file) AS files,
                (SELECT COUNT(*) FROM metadata_hash_registry) AS hashes,
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
        "E2UDE_RUNTIME__UNZIP_WORKERS": 1,
        "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
        "E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE": 2,
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
from e2ude_core.db.models import ArchiveMetadata, FileHashRegistry, FileMetadata
from e2ude_core.db.setup import initialize_database
from e2ude_core.pipelines.scanner import SCANNER_VERSION

zip_path = Path(os.environ["ZIP_PATH"])
missing_member = os.environ["MISSING_MEMBER"]
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
        .values(md5=hashlib.md5(b"missing member content").digest())
        .returning(FileHashRegistry.id)
    ).scalar_one()
    conn.execute(
        sa.insert(FileMetadata).values(
            archive_id=archive_id,
            hash_id=hash_id,
            relative_path=missing_member,
            file_type="SEGMENTS",
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
                      AND target_table = 'rsmdata_segments') AS error_jobs
            """
        ).fetchone()

    assert dict(row) == {
        "artifacts": 0,
        "segment_rows": 0,
        "error_sessions": 1,
        "error_jobs": 1,
    }


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

    with eng.begin() as conn:
        hash_id = conn.execute(
            sa.insert(FileHashRegistry)
            .values(md5=b"\x01" * 16)
            .returning(FileHashRegistry.id)
        ).scalar_one()

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
        PipelineId("atomicity_test"),
        1,
        parse_bad_output,
        (SegmentsData, TmptrData),
    )

    with pytest.raises(Exception):
        process_file(
            eng=eng,
            spec=spec,
            hash_id=hash_id,
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
