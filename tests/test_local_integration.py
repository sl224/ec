from __future__ import annotations

import json
import subprocess
import shutil
import sys
import uuid
from io import BytesIO
from zipfile import ZipFile

import pytest


pytestmark = [pytest.mark.local_integration]


def _first_fixture_zip(fixture_root):
    return next(iter(sorted(fixture_root.rglob("*TransportRSM.fpkg.e2d.zip"))))


def _stage_fixture_subset(local_fixture_root, tmp_path):
    fixture_zip = _first_fixture_zip(local_fixture_root)
    scan_root = tmp_path / "scan_root"
    scan_root.mkdir(parents=True, exist_ok=True)
    staged_zip = scan_root / fixture_zip.name
    shutil.copy2(fixture_zip, staged_zip)
    return staged_zip, scan_root


def _write_transport_rsm_zip(zip_path, *, tmptr_payload: str):
    raw_archive_name = zip_path.name.replace(
        "_TransportRSM.fpkg.e2d.zip", "_RSM_RawArchive.zip"
    )
    nested_buffer = BytesIO()
    with ZipFile(nested_buffer, "w") as nested_zip:
        nested_zip.writestr("RSM/TMPTR_LOG", tmptr_payload)

    with ZipFile(zip_path, "w") as outer_zip:
        outer_zip.writestr(raw_archive_name, nested_buffer.getvalue())


def test_fixture_pipeline_smoke_sqlite(local_fixture_root, run_repo_python, tmp_path):
    fixture_zip = _first_fixture_zip(local_fixture_root)
    sqlite_path = tmp_path / "fixture_smoke.sqlite3"
    script = """
import json
import os
from pathlib import Path
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import LcsTemp, PfcDb
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.orchestration.workflow import process_staged_directory
from e2ude_core.services.zip_io import UnzipContext

zip_path = Path(os.environ["E2UDE_TEST_FIXTURE_ZIP"])
eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
folder_id = register_folders_bulk(eng, [zip_path])[zip_path]

with UnzipContext(zip_path) as ctx:
    process_staged_directory(eng, folder_id, Path(ctx.temp_dir), EtlContext.capture())

with eng.connect() as conn:
    counts = {
        "metadata_file": conn.execute(sa.text("SELECT COUNT(*) FROM metadata_file")).scalar_one(),
        "metadata_hash_registry": conn.execute(sa.text("SELECT COUNT(*) FROM metadata_hash_registry")).scalar_one(),
        "processing_jobs": conn.execute(sa.text("SELECT COUNT(*) FROM processing_jobs")).scalar_one(),
        "metadata_artifact_manifest": conn.execute(sa.text("SELECT COUNT(*) FROM metadata_artifact_manifest")).scalar_one(),
    }
    pfc_row = conn.execute(
        sa.select(
            PfcDb.__table__.c["System TimeStamp"],
            PfcDb.__table__.c["Processed Fault Code"],
            PfcDb.__table__.c["Subsystem"],
            PfcDb.__table__.c["Mission Critical Result"],
        ).limit(1)
    ).first()
    lcs_nulls = conn.execute(
        sa.select(sa.func.count()).select_from(LcsTemp.__table__).where(
            LcsTemp.__table__.c["LCS Time"].is_(None)
        )
    ).scalar_one()

print(
    json.dumps(
        {
            "counts": counts,
            "pfc_row": [None if value is None else str(value) for value in pfc_row],
            "lcs_nulls": lcs_nulls,
        }
    )
)
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_DATABASE__TYPE": "sqlite3",
            "E2UDE_DATABASE__DB_LOCATION": sqlite_path,
            "E2UDE_DATABASE__IN_MEMORY": "false",
            "E2UDE_TEST_FIXTURE_ZIP": fixture_zip,
        },
    )
    payload = json.loads(result.stdout.strip())
    counts = payload["counts"]

    assert counts["metadata_file"] > 0
    assert counts["metadata_hash_registry"] > 0
    assert counts["processing_jobs"] > 0
    assert counts["metadata_artifact_manifest"] > 0
    assert payload["pfc_row"][0] is not None
    assert payload["pfc_row"][1] is not None
    assert payload["pfc_row"][2] is not None
    assert payload["pfc_row"][3] is not None
    assert payload["lcs_nulls"] == 0


def test_main_pipeline_smoke_sqlite(
    local_fixture_root, run_repo_command, run_repo_python, write_app_config, tmp_path
):
    _, scan_root = _stage_fixture_subset(local_fixture_root, tmp_path)
    sqlite_path = tmp_path / "main_smoke.sqlite3"
    staging_root = tmp_path / "staging"
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

    run_repo_command(
        [sys.executable, "-m", "e2ude_core.main"],
        {
            "E2UDE_CONFIG_PATH": config_path,
            "E2UDE_RUNTIME__DISCOVERY_WORKERS": 8,
            "E2UDE_RUNTIME__UNZIP_WORKERS": 2,
            "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
            "E2UDE_RUNTIME__DB_WRITE_WORKERS": 1,
            "E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE": 2,
        },
    )

    payload = json.loads(
        run_repo_python(
            """
import json
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

eng = get_engine(settings.database)
with eng.connect() as conn:
    counts = {
        "metadata_file": conn.execute(sa.text("SELECT COUNT(*) FROM metadata_file")).scalar_one(),
        "metadata_hash_registry": conn.execute(sa.text("SELECT COUNT(*) FROM metadata_hash_registry")).scalar_one(),
        "processing_jobs": conn.execute(sa.text("SELECT COUNT(*) FROM processing_jobs")).scalar_one(),
        "metadata_artifact_manifest": conn.execute(sa.text("SELECT COUNT(*) FROM metadata_artifact_manifest")).scalar_one(),
        "rsmdata_mc_pfc_db": conn.execute(sa.text("SELECT COUNT(*) FROM rsmdata_mc_pfc_db")).scalar_one(),
    }
    pfc_row = conn.execute(
        sa.text(
            "SELECT TOP 1 [System TimeStamp], [Processed Fault Code], [Subsystem], [Mission Critical Result] "
            "FROM rsmdata_mc_pfc_db"
        ) if settings.database.type == "mssql" else sa.text(
            'SELECT "System TimeStamp", "Processed Fault Code", "Subsystem", "Mission Critical Result" '
            'FROM rsmdata_mc_pfc_db LIMIT 1'
        )
    ).first()
    lcs_nulls = conn.execute(
        sa.text('SELECT COUNT(*) FROM rsmdata_mc_lcs_temp WHERE "LCS Time" IS NULL')
    ).scalar_one()
    session_statuses = conn.execute(
        sa.text("SELECT status FROM processing_sessions ORDER BY id")
    ).fetchall()

print(
    json.dumps(
        {
            "counts": counts,
            "pfc_row": [None if value is None else str(value) for value in pfc_row],
            "lcs_nulls": lcs_nulls,
            "session_statuses": [row.status if hasattr(row, 'status') else row[0] for row in session_statuses],
        }
    )
)
""",
            {
                "E2UDE_CONFIG_PATH": config_path,
            },
        ).stdout.strip()
    )

    counts = payload["counts"]
    assert counts["metadata_file"] > 0
    assert counts["metadata_hash_registry"] > 0
    assert counts["processing_jobs"] > 0
    assert counts["metadata_artifact_manifest"] > 0
    assert counts["rsmdata_mc_pfc_db"] > 0
    assert payload["pfc_row"][0] is not None
    assert payload["pfc_row"][1] is not None
    assert payload["pfc_row"][2] is not None
    assert payload["pfc_row"][3] is not None
    assert payload["lcs_nulls"] == 0
    assert "COMPLETED" in payload["session_statuses"]


@pytest.mark.local_mssql
def test_fixture_pipeline_smoke_mssql(
    local_fixture_root,
    local_mssql_server,
    local_mssql_database,
    local_mssql_driver,
    run_repo_python,
):
    fixture_zip = _first_fixture_zip(local_fixture_root)
    schema_name = f"e2ude_core_test_{uuid.uuid4().hex[:8]}"
    script = """
import json
import os
from pathlib import Path
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.base_session import Base, DEFAULT_SCHEMA
from e2ude_core.db.models import LcsTemp, PfcDb
from e2ude_core.db.setup import initialize_database, register_folders_bulk
from e2ude_core.orchestration.workflow import process_staged_directory
from e2ude_core.services.zip_io import UnzipContext

zip_path = Path(os.environ["E2UDE_TEST_FIXTURE_ZIP"])
eng = get_engine(settings.database)

try:
    initialize_database(eng, reset_tables=False)
    folder_id = register_folders_bulk(eng, [zip_path])[zip_path]

    with UnzipContext(zip_path) as ctx:
        process_staged_directory(eng, folder_id, Path(ctx.temp_dir), EtlContext.capture())

    with eng.connect() as conn:
        counts = {
            "metadata_file": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[metadata_file]")).scalar_one(),
            "metadata_hash_registry": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[metadata_hash_registry]")).scalar_one(),
            "processing_jobs": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[processing_jobs]")).scalar_one(),
            "metadata_artifact_manifest": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[metadata_artifact_manifest]")).scalar_one(),
        }
        pfc_row = conn.execute(
            sa.select(
                PfcDb.__table__.c["System TimeStamp"],
                PfcDb.__table__.c["Processed Fault Code"],
                PfcDb.__table__.c["Subsystem"],
                PfcDb.__table__.c["Mission Critical Result"],
            ).limit(1)
        ).first()
        lcs_nulls = conn.execute(
            sa.select(sa.func.count()).select_from(LcsTemp.__table__).where(
                LcsTemp.__table__.c["LCS Time"].is_(None)
            )
        ).scalar_one()
finally:
    Base.metadata.drop_all(eng)
    with eng.begin() as conn:
        conn.execute(sa.text(f"DROP SCHEMA [{DEFAULT_SCHEMA}]"))

print(
    json.dumps(
        {
            "counts": counts,
            "pfc_row": [None if value is None else str(value) for value in pfc_row],
            "lcs_nulls": lcs_nulls,
        }
    )
)
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_DATABASE__TYPE": "mssql",
            "E2UDE_DATABASE__SERVER_NAME": local_mssql_server,
            "E2UDE_DATABASE__DB_NAME": local_mssql_database,
            "E2UDE_DATABASE__DRIVER": local_mssql_driver,
            "E2UDE_DATABASE__TRUSTED_CONNECTION": "yes",
            "E2UDE_DATABASE__SCHEMA_NAME": schema_name,
            "E2UDE_TEST_FIXTURE_ZIP": fixture_zip,
        },
    )
    payload = json.loads(result.stdout.strip())
    counts = payload["counts"]

    assert counts["metadata_file"] > 0
    assert counts["metadata_hash_registry"] > 0
    assert counts["processing_jobs"] > 0
    assert counts["metadata_artifact_manifest"] > 0
    assert payload["pfc_row"][0] is not None
    assert payload["pfc_row"][1] is not None
    assert payload["pfc_row"][2] is not None
    assert payload["pfc_row"][3] is not None
    assert payload["lcs_nulls"] == 0


@pytest.mark.local_mssql
def test_main_pipeline_blue_green_mssql(
    local_fixture_root,
    local_mssql_server,
    local_mssql_database,
    local_mssql_driver,
    run_repo_command,
    run_repo_python,
    write_app_config,
    tmp_path,
):
    _, scan_root = _stage_fixture_subset(local_fixture_root, tmp_path)
    staging_root = tmp_path / "staging"
    candidate_schema = f"e2ude_candidate_{uuid.uuid4().hex[:8]}"
    stable_schema = f"e2ude_prod_{uuid.uuid4().hex[:8]}"
    archive_schema = f"e2ude_archive_{uuid.uuid4().hex[:8]}"
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": local_mssql_server,
            "db_name": local_mssql_database,
            "driver": local_mssql_driver,
            "trusted_connection": "yes",
            "schema_name": candidate_schema,
        },
        paths={
            "scan_root": scan_root,
            "staging_root": staging_root,
        },
    )

    common_env = {
        "E2UDE_CONFIG_PATH": config_path,
    }

    try:
        run_repo_python(
            """
import os
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

stable_schema = os.environ["STABLE_SCHEMA"]
eng = get_engine(settings.database)
with eng.begin() as conn:
    conn.execute(sa.text(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{stable_schema}') EXEC('CREATE SCHEMA [{stable_schema}]')"))
    conn.execute(sa.text(f"CREATE TABLE [{stable_schema}].[promotion_marker] (marker INT NOT NULL)"))
    conn.execute(sa.text(f"INSERT INTO [{stable_schema}].[promotion_marker] (marker) VALUES (1)"))
""",
            {
                **common_env,
                "E2UDE_DATABASE__SCHEMA_NAME": candidate_schema,
                "STABLE_SCHEMA": stable_schema,
            },
        )

        run_repo_command(
            [sys.executable, "-m", "e2ude_core.main"],
            {
                **common_env,
                "E2UDE_RUNTIME__DISCOVERY_WORKERS": 8,
                "E2UDE_RUNTIME__UNZIP_WORKERS": 2,
                "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
                "E2UDE_RUNTIME__DB_WRITE_WORKERS": 1,
                "E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE": 2,
            },
        )

        run_repo_command(
            [
                sys.executable,
                "scripts/promote_schema.py",
                "--source-schema",
                candidate_schema,
                "--target-schema",
                stable_schema,
                "--archive-schema",
                archive_schema,
                "--yes",
                "--confirm-target-schema",
                stable_schema,
            ],
            {
                **common_env,
                "E2UDE_DATABASE__SCHEMA_NAME": candidate_schema,
            },
        )

        payload = json.loads(
            run_repo_python(
                """
import json
import os
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

stable_schema = os.environ["STABLE_SCHEMA"]
archive_schema = os.environ["ARCHIVE_SCHEMA"]
candidate_schema = os.environ["CANDIDATE_SCHEMA"]

eng = get_engine(settings.database)
with eng.connect() as conn:
    counts = {
        "metadata_file": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{stable_schema}].[metadata_file]")).scalar_one(),
        "metadata_hash_registry": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{stable_schema}].[metadata_hash_registry]")).scalar_one(),
        "processing_jobs": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{stable_schema}].[processing_jobs]")).scalar_one(),
        "metadata_artifact_manifest": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{stable_schema}].[metadata_artifact_manifest]")).scalar_one(),
        "rsmdata_mc_pfc_db": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{stable_schema}].[rsmdata_mc_pfc_db]")).scalar_one(),
    }
    pfc_row = conn.execute(
        sa.text(
            f"SELECT TOP 1 [System TimeStamp], [Processed Fault Code], [Subsystem], [Mission Critical Result] "
            f"FROM [{stable_schema}].[rsmdata_mc_pfc_db]"
        )
    ).first()
    lcs_nulls = conn.execute(
        sa.text(f"SELECT COUNT(*) FROM [{stable_schema}].[rsmdata_mc_lcs_temp] WHERE [LCS Time] IS NULL")
    ).scalar_one()
    archive_marker = conn.execute(
        sa.text(f"SELECT COUNT(*) FROM [{archive_schema}].[promotion_marker]")
    ).scalar_one()
    candidate_tables = conn.execute(
        sa.text(
            \"\"\"
            SELECT COUNT(*)
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name
            \"\"\"
        ),
        {"schema_name": candidate_schema},
    ).scalar_one()

print(
    json.dumps(
        {
            "counts": counts,
            "pfc_row": [None if value is None else str(value) for value in pfc_row],
            "lcs_nulls": lcs_nulls,
            "archive_marker": archive_marker,
            "candidate_tables": candidate_tables,
        }
    )
)
""",
                {
                    **common_env,
                    "E2UDE_DATABASE__SCHEMA_NAME": stable_schema,
                    "STABLE_SCHEMA": stable_schema,
                    "ARCHIVE_SCHEMA": archive_schema,
                    "CANDIDATE_SCHEMA": candidate_schema,
                },
            ).stdout.strip()
        )

        counts = payload["counts"]
        assert counts["metadata_file"] > 0
        assert counts["metadata_hash_registry"] > 0
        assert counts["processing_jobs"] > 0
        assert counts["metadata_artifact_manifest"] > 0
        assert counts["rsmdata_mc_pfc_db"] > 0
        assert payload["pfc_row"][0] is not None
        assert payload["pfc_row"][1] is not None
        assert payload["pfc_row"][2] is not None
        assert payload["pfc_row"][3] is not None
        assert payload["lcs_nulls"] == 0
        assert payload["archive_marker"] == 1
        assert payload["candidate_tables"] == 0

        run_repo_command(
            [
                sys.executable,
                "scripts/cleanup_mssql_schema.py",
                "--schema-name",
                stable_schema,
                "--preview",
            ],
            {
                **common_env,
                "E2UDE_DATABASE__SCHEMA_NAME": stable_schema,
            },
        )
    finally:
        for schema_name in (candidate_schema, stable_schema, archive_schema):
            run_repo_command(
                [
                    sys.executable,
                    "scripts/cleanup_mssql_schema.py",
                    "--schema-name",
                    schema_name,
                    "--yes",
                    "--confirm-schema",
                    schema_name,
                ],
                {
                    **common_env,
                    "E2UDE_DATABASE__SCHEMA_NAME": schema_name,
                },
            )


@pytest.mark.local_mssql
def test_cleanup_schema_script_requires_exact_confirmation(
    local_mssql_server,
    local_mssql_database,
    local_mssql_driver,
    run_repo_command,
    run_repo_python,
    write_app_config,
    tmp_path,
):
    schema_name = f"e2ude_tmp_{uuid.uuid4().hex[:8]}"
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": local_mssql_server,
            "db_name": local_mssql_database,
            "driver": local_mssql_driver,
            "trusted_connection": "yes",
            "schema_name": schema_name,
        },
        paths={
            "scan_root": tmp_path / "scan_root",
            "staging_root": tmp_path / "staging",
        },
    )
    common_env = {
        "E2UDE_CONFIG_PATH": config_path,
    }

    try:
        run_repo_python(
            """
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

schema_name = settings.database.schema_name
eng = get_engine(settings.database)
with eng.begin() as conn:
    conn.execute(sa.text(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}') EXEC('CREATE SCHEMA [{schema_name}]')"))
    conn.execute(sa.text(f"CREATE TABLE [{schema_name}].[cleanup_marker] (marker INT NOT NULL)"))
    conn.execute(sa.text(f"INSERT INTO [{schema_name}].[cleanup_marker] (marker) VALUES (1)"))
""",
            common_env,
        )

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            run_repo_command(
                [
                    sys.executable,
                    "scripts/cleanup_mssql_schema.py",
                    "--schema-name",
                    schema_name,
                    "--yes",
                ],
                {
                    **common_env,
                    "E2UDE_DATABASE__SCHEMA_NAME": schema_name,
                },
            )

        failure_output = (exc_info.value.stdout or "") + (exc_info.value.stderr or "")
        assert "--confirm-schema" in failure_output

        payload = json.loads(
            run_repo_python(
                """
import json
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine

schema_name = settings.database.schema_name
eng = get_engine(settings.database)
with eng.connect() as conn:
    table_count = conn.execute(
        sa.text(
            \"\"\"
            SELECT COUNT(*)
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = :schema_name
            \"\"\"
        ),
        {"schema_name": schema_name},
    ).scalar_one()

print(json.dumps({"table_count": table_count}))
""",
                common_env,
            ).stdout.strip()
        )

        assert payload["table_count"] == 1
    finally:
        run_repo_command(
            [
                sys.executable,
                "scripts/cleanup_mssql_schema.py",
                "--schema-name",
                schema_name,
                "--yes",
                "--confirm-schema",
                schema_name,
            ],
            {
                **common_env,
                "E2UDE_DATABASE__SCHEMA_NAME": schema_name,
            },
        )


@pytest.mark.local_mssql
def test_main_pipeline_parallel_duplicate_tmptr_hashes_are_serialized(
    local_mssql_server,
    local_mssql_database,
    local_mssql_driver,
    run_repo_command,
    run_repo_python,
    write_app_config,
    tmp_path,
):
    scan_root = tmp_path / "scan_root"
    staging_root = tmp_path / "staging"
    scan_root.mkdir(parents=True, exist_ok=True)

    line_count = 400
    tmptr_payload = "\n".join(
        [
            (
                f"AFMC1,20231003,04:59:09.{i:03d},IPM & P2 Connectors,"
                f"{20 + (i % 10):03d}C,{70 + (i % 10):03d}F"
            )
            for i in range(line_count)
        ]
    )

    zip_count = 8
    for i in range(zip_count):
        zip_path = scan_root / (
            f"169871_202311{7 + i:02d}_024218_{i:03d}_TransportRSM.fpkg.e2d.zip"
        )
        _write_transport_rsm_zip(zip_path, tmptr_payload=tmptr_payload)

    schema_name = f"e2ude_tmptr_{uuid.uuid4().hex[:8]}"
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": local_mssql_server,
            "db_name": local_mssql_database,
            "driver": local_mssql_driver,
            "trusted_connection": "yes",
            "schema_name": schema_name,
        },
        paths={
            "scan_root": scan_root,
            "staging_root": staging_root,
        },
    )

    common_env = {
        "E2UDE_CONFIG_PATH": config_path,
    }

    try:
        run_repo_command(
            [sys.executable, "-m", "e2ude_core.main"],
            {
                **common_env,
                "E2UDE_RUNTIME__DISCOVERY_WORKERS": 4,
                "E2UDE_RUNTIME__UNZIP_WORKERS": 4,
                "E2UDE_RUNTIME__PROCESS_WORKERS": 4,
                "E2UDE_RUNTIME__DB_WRITE_WORKERS": 1,
                "E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE": 4,
            },
        )

        payload = json.loads(
            run_repo_python(
                """
import json
import sqlalchemy as sa
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.base_session import DEFAULT_SCHEMA

eng = get_engine(settings.database)
with eng.connect() as conn:
    counts = {
        "sessions": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[processing_sessions]")).scalar_one(),
        "error_sessions": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[processing_sessions] WHERE status = 'ERROR'")).scalar_one(),
        "error_jobs": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[processing_jobs] WHERE status = 'ERROR'")).scalar_one(),
        "hashes": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[metadata_hash_registry]")).scalar_one(),
        "artifacts": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[metadata_artifact_manifest] WHERE target_table = 'rsmdata_tmptr'")).scalar_one(),
        "tmptr_rows": conn.execute(sa.text(f"SELECT COUNT(*) FROM [{DEFAULT_SCHEMA}].[rsmdata_tmptr]")).scalar_one(),
        "distinct_tmptr_hashes": conn.execute(sa.text(f"SELECT COUNT(DISTINCT hash_id) FROM [{DEFAULT_SCHEMA}].[rsmdata_tmptr]")).scalar_one(),
    }
    tmptr_job_statuses = conn.execute(
        sa.text(
            f"SELECT status FROM [{DEFAULT_SCHEMA}].[processing_jobs] "
            f"WHERE pipeline_id = 'tmptr_log' ORDER BY id"
        )
    ).fetchall()

print(
    json.dumps(
        {
            "counts": counts,
            "tmptr_job_statuses": [row.status if hasattr(row, 'status') else row[0] for row in tmptr_job_statuses],
        }
    )
)
""",
                common_env,
            ).stdout.strip()
        )

        counts = payload["counts"]
        assert counts["sessions"] == zip_count
        assert counts["error_sessions"] == 0
        assert counts["error_jobs"] == 0
        assert counts["hashes"] == 1
        assert counts["artifacts"] == 1
        assert counts["distinct_tmptr_hashes"] == 1
        assert counts["tmptr_rows"] == line_count
        assert payload["tmptr_job_statuses"]
        assert all(status == "COMPLETED" for status in payload["tmptr_job_statuses"])
    finally:
        run_repo_command(
            [
                sys.executable,
                "scripts/cleanup_mssql_schema.py",
                "--schema-name",
                schema_name,
                "--yes",
                "--confirm-schema",
                schema_name,
            ],
            {
                **common_env,
                "E2UDE_DATABASE__SCHEMA_NAME": schema_name,
            },
        )
