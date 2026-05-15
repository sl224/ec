from __future__ import annotations

import json
import sys
from zipfile import ZipFile


def test_preview_parser_auto_detects_registered_handler(run_repo_command, tmp_path):
    mcdata_file = tmp_path / "sample_MCData"
    mcdata_file.write_text(
        "\n".join(
            [
                (
                    "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,"
                    "COMM,CI,,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,"
                    ",,,,,,,,,False,True,,,False,,,,,,,,,,"
                ),
                (
                    "2,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,"
                    ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
                ),
            ]
        ),
        encoding="utf-8",
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "preview",
            str(mcdata_file),
            "--head",
            "1",
        ]
    )
    payload = json.loads(result.stdout)

    assert payload["selected_file_type"] == "MCDATA"
    assert payload["parser_id"] == "mcdata"
    assert any(
        table["table"] == "rsmdata_mc_pfc_db" and table["rows"] == 1
        for table in payload["tables"]
    )


def test_preview_parser_supports_explicit_file_type_override(run_repo_command, tmp_path):
    segments_file = tmp_path / "segments_preview_input.txt"
    segments_file.write_text(
        (
            "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,"
            "01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,"
            "false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,"
            "false,0,0,,,\n"
        ),
        encoding="utf-8",
    )

    result = run_repo_command(
        [
            sys.executable,
            "-m",
            "e2ude_core.cli",
            "parser",
            "preview",
            str(segments_file),
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
    assert payload["tables"][0]["rows"] == 1
    assert payload["tables"][0]["preview"][0]["flight_status"] == "PreFlight"


def test_run_fixture_zip_e2e_reports_materialized_tables_on_sqlite(
    run_repo_command, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "fixture_e2e.sqlite3"
    config_path = write_app_config(
        database={
            "type": "sqlite3",
            "db_location": sqlite_path,
            "in_memory": False,
        },
        paths={
            "scan_root": tmp_path / "unused_scan_root",
            "staging_root": tmp_path / "staging",
        },
    )

    zip_path = tmp_path / "169871_20231107_024218_987_TransportRSM.fpkg.e2d.zip"
    with ZipFile(zip_path, "w") as zip_file:
        zip_file.writestr(
            "169871_20231107_024218_987_MCData",
            "\n".join(
                [
                    (
                        "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,"
                        "COMM,CI,,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,"
                        ",,,,,,,,,False,True,,,False,,,,,,,,,,"
                    ),
                    (
                        "2,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,"
                        ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
                    ),
                ]
            ),
        )

    result = run_repo_command(
        [sys.executable, "scripts/run_fixture_zip_e2e.py", str(zip_path)],
        {"E2UDE_CONFIG_PATH": config_path},
    )
    payload = json.loads(result.stdout[result.stdout.find("{") :])

    assert payload["database_type"] == "sqlite3"
    assert payload["run_status"]["session_status"] == "COMPLETED"
    assert payload["run_status"]["error_jobs"] == 0
    assert payload["materialized_tables"]["rsmdata_mc_pfc_db"] == 1
    assert payload["materialized_tables"]["rsmdata_mc_lcs_temp"] == 1
