from __future__ import annotations

import json
import os
import tempfile
from pathlib import PureWindowsPath

import pytest
import sqlalchemy as sa

import e2ude_core.db.setup as db_setup
import e2ude_core.main as app_main
from e2ude_core.db.models import ArchiveMetadata, SegmentsData
from e2ude_core.db.models import DiscoveryDirectoryMetadata
from e2ude_core.db.schema_safety import (
    SchemaClassification,
    format_target_banner,
    is_disposable_schema,
    is_protected_schema,
    schema_classification,
)
from e2ude_core.runtime_files import (
    CURRENT_METADATA_CATALOG_GENERATION,
    HANDLED_FILE_SPECS_BY_TYPE,
    FileType,
    PipelineId,
    RuntimeFileSpec,
    build_active_stage_patterns,
    compute_metadata_catalog_generation,
)
from e2ude_core.pipelines.scanner import SCANNER_VERSION
from e2ude_core.services.discovery import (
    DiscoveryDirectorySnapshot,
    DiscoveryMode,
    discover_archives,
)


def test_active_stage_patterns_include_nested_archive_dependency_for_runtime_handlers():
    patterns = build_active_stage_patterns(
        sorted(HANDLED_FILE_SPECS_BY_TYPE, key=lambda file_type: file_type.value)
    )

    assert "*_MCData" in patterns
    assert "*_Segments" in patterns
    assert "*_RSM_RawArchive/RSM/TMPTR_LOG" in patterns
    assert "*RSM_RawArchive.zip" in patterns
    assert "*_Versions.xml" not in patterns


def test_scanner_version_tracks_derived_metadata_catalog_generation():
    assert SCANNER_VERSION == CURRENT_METADATA_CATALOG_GENERATION
    assert SCANNER_VERSION > 0


def test_metadata_catalog_generation_changes_when_file_type_becomes_active():
    def parse_noop(_path):
        return {}

    unhandled_generation = compute_metadata_catalog_generation(
        [RuntimeFileSpec(FileType.ENGINE, ("*_Engine",))],
        stage_dependencies=(),
    )
    handled_generation = compute_metadata_catalog_generation(
        [
            RuntimeFileSpec(
                FileType.ENGINE,
                ("*_Engine",),
                PipelineId("engine"),
                1,
                parse_noop,
                (SegmentsData,),
            )
        ],
        stage_dependencies=(),
    )
    handler_bump_generation = compute_metadata_catalog_generation(
        [
            RuntimeFileSpec(
                FileType.ENGINE,
                ("*_Engine",),
                PipelineId("engine"),
                2,
                parse_noop,
                (SegmentsData,),
            )
        ],
        stage_dependencies=(),
    )

    assert handled_generation != unhandled_generation
    assert handler_bump_generation == handled_generation


def test_schema_safety_marks_shared_and_disposable_names_by_convention():
    assert is_protected_schema("e2ude_core")
    assert is_protected_schema("e2ude_core_dev")
    assert not is_protected_schema("e2ude_candidate_1234")

    assert is_disposable_schema("e2ude_tmp_1234")
    assert is_disposable_schema("e2ude_core_fixture_validation")
    assert is_disposable_schema("e2ude_archive_1234")
    assert schema_classification("e2ude_core") == SchemaClassification.PROTECTED_SHARED
    assert (
        schema_classification("e2ude_core_fixture_validation")
        == SchemaClassification.DISPOSABLE
    )
    assert schema_classification("custom_schema") == SchemaClassification.CUSTOM


def test_format_target_banner_includes_db_target_and_classification():
    class FakeDbSettings:
        server_name = "localhost"
        db_name = "AnalyticsDataMart"

    banner = format_target_banner(FakeDbSettings(), schema_name="e2ude_core_dev")

    assert "server=[localhost]" in banner
    assert "database=[AnalyticsDataMart]" in banner
    assert "schema=[e2ude_core_dev]" in banner
    assert "classification=[PROTECTED_SHARED]" in banner


def test_main_exits_nonzero_on_fatal_error(monkeypatch, tmp_path):
    class DummyEngine:
        def dispose(self):
            return None

    monkeypatch.setattr(app_main, "_resolve_staging_root", lambda: tmp_path / "staging")
    monkeypatch.setattr(app_main, "setup_logging", lambda _settings: None)
    monkeypatch.setattr(app_main.sql_io, "get_engine", lambda *args, **kwargs: DummyEngine())
    monkeypatch.setattr(
        app_main,
        "initialize_database",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(SystemExit) as exc_info:
        app_main.main()

    assert exc_info.value.code == 1


def test_committed_defaults_toml_provides_scan_root(run_repo_python, tmp_path):
    missing_config = tmp_path / "missing_config.toml"

    script = """
import json
from e2ude_core.config import settings

print(json.dumps({"scan_root": str(settings.paths.scan_root)}))
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_CONFIG_PATH": missing_config,
        },
    )
    payload = json.loads(result.stdout.strip())

    actual_scan_root = str(PureWindowsPath(payload["scan_root"]))
    expected_scan_root = str(PureWindowsPath(r"\\Rsiny1-ilsfs\RSM"))

    assert actual_scan_root.rstrip("\\") == expected_scan_root.rstrip("\\")


def test_nested_runtime_and_path_env_overrides_are_applied(run_repo_python, tmp_path):
    override_staging = tmp_path / "staging_override"

    script = """
import json
from e2ude_core.config import settings

print(
    json.dumps(
        {
            "process_workers": settings.runtime.process_workers,
            "staging_root": str(settings.paths.staging_root),
            "viztracer": settings.diagnostics.enable_viztracer,
        }
    )
)
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_RUNTIME__PROCESS_WORKERS": "3",
            "E2UDE_PATHS__STAGING_ROOT": override_staging,
            "E2UDE_DIAGNOSTICS__ENABLE_VIZTRACER": "true",
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload["process_workers"] == 3
    assert str(PureWindowsPath(payload["staging_root"])) == str(
        PureWindowsPath(str(override_staging))
    )
    assert payload["viztracer"] is True


def test_default_staging_root_uses_os_temp_area(run_repo_python, tmp_path):
    missing_config = tmp_path / "missing_config.toml"

    script = """
import json
from e2ude_core.config import settings

print(json.dumps({"staging_root": str(settings.paths.staging_root)}))
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_CONFIG_PATH": missing_config,
        },
    )
    payload = json.loads(result.stdout.strip())

    expected_root = PureWindowsPath(tempfile.gettempdir()) / "e2ude_core_staging"
    actual_root = PureWindowsPath(payload["staging_root"])

    assert actual_root == expected_root


def test_mssql_schema_override_keeps_database_discriminator_intact(
    run_repo_python, write_app_config
):
    config_path = write_app_config(
        database={
            "type": "mssql",
            "server_name": "localhost",
            "db_name": "AnalyticsDataMart",
            "driver": "ODBC Driver 17 for SQL Server",
            "trusted_connection": "yes",
            "schema_name": "e2ude_core_dev",
        }
    )

    script = """
import json
from e2ude_core.config import settings

print(
    json.dumps(
        {
            "db_type": settings.database.type,
            "schema_name": settings.database.schema_name,
        }
    )
)
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_CONFIG_PATH": config_path,
            "E2UDE_DATABASE__TYPE": "mssql",
            "E2UDE_DATABASE__SCHEMA_NAME": "e2ude_core",
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload["db_type"] == "mssql"
    assert payload["schema_name"] == "e2ude_core"


def test_register_archives_bulk_keeps_same_second_archives_distinct(
    run_repo_python, tmp_path
):
    sqlite_path = tmp_path / "register.sqlite3"
    zip_a = tmp_path / "169871_20231107_024218_025_TransportRSM.fpkg.e2d.zip"
    zip_b = tmp_path / "169871_20231107_024218_999_TransportRSM.fpkg.e2d.zip"

    script = """
import json
import os
from pathlib import Path
from sqlalchemy import text
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.setup import initialize_database, register_archives_bulk

zip_a = Path(os.environ["ZIP_A"])
zip_b = Path(os.environ["ZIP_B"])

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
archive_map = register_archives_bulk(eng, [zip_a, zip_b])

with eng.connect() as conn:
    rows = conn.execute(
        text(
            "SELECT id, source_path, source_size_bytes, source_mtime_ns, "
            "required_scan_version, completed_scan_version "
            "FROM metadata_archive ORDER BY id"
        )
    ).fetchall()

print(
    json.dumps(
        {
            "archive_ids": [archive_map[zip_a], archive_map[zip_b]],
            "row_count": len(rows),
            "paths": [row.source_path for row in rows],
            "source_sizes": [row.source_size_bytes for row in rows],
            "source_mtimes": [row.source_mtime_ns for row in rows],
            "required_scan_versions": [row.required_scan_version for row in rows],
            "completed_scan_versions": [row.completed_scan_version for row in rows],
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
            "ZIP_A": zip_a,
            "ZIP_B": zip_b,
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload["row_count"] == 2
    assert payload["archive_ids"][0] != payload["archive_ids"][1]
    assert str(zip_a) in payload["paths"]
    assert str(zip_b) in payload["paths"]
    assert payload["required_scan_versions"] == [SCANNER_VERSION, SCANNER_VERSION]
    assert payload["completed_scan_versions"] == [0, 0]


def test_register_archives_bulk_chunks_large_existing_path_lookups(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "register_chunk.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    ArchiveMetadata.__table__.create(eng)

    zip_paths = [
        tmp_path / f"16987{idx}_20231107_02421{idx}_{idx:03d}_TransportRSM.fpkg.e2d.zip"
        for idx in range(5)
    ]

    seen_batches: list[list[str]] = []
    original_iter = db_setup._iter_path_batches

    def recording_batches(paths, batch_size=None):
        for batch in original_iter(paths, batch_size=batch_size):
            seen_batches.append(list(batch))
            yield batch

    monkeypatch.setattr(db_setup, "ARCHIVE_LOOKUP_BATCH_SIZE", 2)
    monkeypatch.setattr(db_setup, "_iter_path_batches", recording_batches)

    archive_map = db_setup.register_archives_bulk(eng, zip_paths)

    assert len(archive_map) == len(zip_paths)
    assert [len(batch) for batch in seen_batches] == [2, 2, 1, 2, 2, 1]


def test_register_archives_bulk_batches_new_archive_inserts(monkeypatch, tmp_path):
    db_path = tmp_path / "register_insert_chunk.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    ArchiveMetadata.__table__.create(eng)

    zip_paths = [
        tmp_path / f"16987{idx}_20231107_02421{idx}_{idx:03d}_TransportRSM.fpkg.e2d.zip"
        for idx in range(5)
    ]

    insert_batch_sizes: list[int] = []

    @sa.event.listens_for(eng, "before_cursor_execute")
    def record_insert_batch(conn, cursor, statement, parameters, context, executemany):
        if not statement.lstrip().lower().startswith("insert into metadata_archive"):
            return
        insert_batch_sizes.append(len(parameters) if executemany else 1)

    monkeypatch.setattr(db_setup, "ARCHIVE_INSERT_BATCH_SIZE", 2)

    archive_map = db_setup.register_archives_bulk(eng, zip_paths)

    assert len(archive_map) == len(zip_paths)
    assert insert_batch_sizes == [2, 2, 1]


def test_register_archives_bulk_skips_unchanged_archive_updates(tmp_path):
    db_path = tmp_path / "register_skip_updates.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    db_setup.initialize_database(eng, reset_tables=True)

    zip_path = tmp_path / "169871_20231107_024218_025_TransportRSM.fpkg.e2d.zip"
    zip_path.write_text("archive", encoding="utf-8")

    db_setup.register_archives_bulk(eng, [zip_path])

    update_count = 0

    @sa.event.listens_for(eng, "before_cursor_execute")
    def record_archive_update(conn, cursor, statement, parameters, context, executemany):
        nonlocal update_count
        if statement.lstrip().lower().startswith("update metadata_archive"):
            update_count += 1

    archive_map = db_setup.register_archives_bulk(eng, [zip_path])

    assert archive_map[zip_path] > 0
    assert update_count == 0


def test_record_directory_snapshots_batches_inserts_and_preserves_scan_time(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "directory_snapshot_chunk.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    DiscoveryDirectoryMetadata.__table__.create(eng)

    snapshots = [
        DiscoveryDirectorySnapshot(
            path=tmp_path / f"dir_{idx}",
            mtime_ns=idx,
            contains_archives=idx % 2 == 0,
            scanned=idx % 2 == 0,
        )
        for idx in range(5)
    ]

    insert_batch_sizes: list[int] = []

    @sa.event.listens_for(eng, "before_cursor_execute")
    def record_insert_batch(conn, cursor, statement, parameters, context, executemany):
        if not statement.lstrip().lower().startswith(
            "insert into metadata_discovery_directory"
        ):
            return
        insert_batch_sizes.append(len(parameters) if executemany else 1)

    monkeypatch.setattr(db_setup, "DIRECTORY_SNAPSHOT_BATCH_SIZE", 2)

    db_setup.record_directory_snapshots(eng, snapshots)

    assert insert_batch_sizes == [2, 2, 1]

    scanned_path = str(snapshots[0].path)
    with eng.connect() as conn:
        first_scan_time = conn.execute(
            sa.select(DiscoveryDirectoryMetadata.last_scanned_at).where(
                DiscoveryDirectoryMetadata.path == scanned_path
            )
        ).scalar_one()

    db_setup.record_directory_snapshots(
        eng,
        [
            DiscoveryDirectorySnapshot(
                path=snapshots[0].path,
                mtime_ns=999,
                contains_archives=True,
                scanned=False,
            )
        ],
    )

    with eng.connect() as conn:
        updated = conn.execute(
            sa.select(
                DiscoveryDirectoryMetadata.mtime_ns,
                DiscoveryDirectoryMetadata.last_scanned_at,
            ).where(DiscoveryDirectoryMetadata.path == scanned_path)
        ).one()

    assert updated.mtime_ns == 999
    assert updated.last_scanned_at == first_scan_time


def test_incremental_discovery_enumerates_known_archives_each_run_for_correctness(
    tmp_path,
):
    db_path = tmp_path / "discovery.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    db_setup.initialize_database(eng, reset_tables=True)

    scan_root = tmp_path / "scan_root"
    leaf_dir = scan_root / "169871" / "2024" / "01"
    leaf_dir.mkdir(parents=True, exist_ok=True)

    zip_a = leaf_dir / "169871_20240101_000000_000_TransportRSM.fpkg.e2d.zip"
    zip_a.write_text("a", encoding="utf-8")

    first = discover_archives(
        scan_root,
        known_directory_states={},
        mode=DiscoveryMode.INCREMENTAL,
        max_workers=4,
    )
    db_setup.record_directory_snapshots(eng, first.directory_snapshots)
    db_setup.register_archives_bulk(
        eng,
        list(first.archives),
        scanned_directory_paths=first.scanned_directory_paths,
        missing_directory_paths=first.missing_directory_paths,
    )

    assert [archive.path for archive in first.archives] == [zip_a]
    assert first.scanned_directory_count >= 4

    second = discover_archives(
        scan_root,
        known_directory_states=db_setup.load_directory_scan_cache(
            eng, root_path=scan_root
        ),
        mode=DiscoveryMode.INCREMENTAL,
        max_workers=4,
    )
    assert [archive.path for archive in second.archives] == [zip_a]
    assert second.archive_directory_scan_count == 1
    assert second.frontier_directory_scan_count >= 1
    assert second.skipped_directory_count >= 1

    year_dir = scan_root / "169871" / "2024"
    original_year_dir_mtime_ns = year_dir.stat().st_mtime_ns
    new_leaf_dir = scan_root / "169871" / "2024" / "02"
    new_leaf_dir.mkdir(parents=True, exist_ok=True)
    zip_b = new_leaf_dir / "169871_20240102_000000_000_TransportRSM.fpkg.e2d.zip"
    zip_b.write_text("b", encoding="utf-8")
    os.utime(
        year_dir,
        ns=(original_year_dir_mtime_ns + 1_000_000_000,) * 2,
    )

    third = discover_archives(
        scan_root,
        known_directory_states=db_setup.load_directory_scan_cache(
            eng, root_path=scan_root
        ),
        mode=DiscoveryMode.INCREMENTAL,
        max_workers=4,
    )

    assert set(archive.path for archive in third.archives) == {zip_a, zip_b}
    assert leaf_dir in third.scanned_directory_paths
    assert new_leaf_dir in third.scanned_directory_paths


def test_incremental_discovery_catches_in_place_archive_edit_when_parent_dir_mtime_is_preserved(
    tmp_path,
):
    db_path = tmp_path / "discovery_edit.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    db_setup.initialize_database(eng, reset_tables=True)

    scan_root = tmp_path / "scan_root"
    leaf_dir = scan_root / "169871" / "2024" / "01"
    leaf_dir.mkdir(parents=True, exist_ok=True)

    zip_a = leaf_dir / "169871_20240101_000000_000_TransportRSM.fpkg.e2d.zip"
    zip_a.write_text("a", encoding="utf-8")

    first = discover_archives(
        scan_root,
        known_directory_states={},
        mode=DiscoveryMode.INCREMENTAL,
        max_workers=4,
    )
    db_setup.record_directory_snapshots(eng, first.directory_snapshots)
    archive_map = db_setup.register_archives_bulk(
        eng,
        list(first.archives),
        scanned_directory_paths=first.scanned_directory_paths,
        missing_directory_paths=first.missing_directory_paths,
    )
    archive_id = archive_map[zip_a]

    with eng.connect() as conn:
        original_row = conn.execute(
            sa.select(
                ArchiveMetadata.source_size_bytes,
                ArchiveMetadata.source_mtime_ns,
            ).where(ArchiveMetadata.id == archive_id)
        ).one()

    original_dir_mtime_ns = leaf_dir.stat().st_mtime_ns
    updated_payload = "archive contents changed"
    zip_a.write_text(updated_payload, encoding="utf-8")
    os.utime(
        zip_a,
        ns=(original_row.source_mtime_ns + 1_000_000_000,) * 2,
    )
    os.utime(leaf_dir, ns=(original_dir_mtime_ns, original_dir_mtime_ns))

    second = discover_archives(
        scan_root,
        known_directory_states=db_setup.load_directory_scan_cache(
            eng, root_path=scan_root
        ),
        mode=DiscoveryMode.INCREMENTAL,
        max_workers=4,
    )
    db_setup.record_directory_snapshots(eng, second.directory_snapshots)
    db_setup.register_archives_bulk(
        eng,
        list(second.archives),
        scanned_directory_paths=second.scanned_directory_paths,
        missing_directory_paths=second.missing_directory_paths,
    )

    with eng.connect() as conn:
        updated_row = conn.execute(
            sa.select(
                ArchiveMetadata.source_size_bytes,
                ArchiveMetadata.source_mtime_ns,
                ArchiveMetadata.completed_scan_version,
            ).where(ArchiveMetadata.id == archive_id)
        ).one()

    assert [archive.path for archive in second.archives] == [zip_a]
    assert second.archive_directory_scan_count == 1
    assert second.frontier_directory_scan_count >= 1
    assert updated_row.source_size_bytes == len(updated_payload)
    assert updated_row.source_mtime_ns > original_row.source_mtime_ns
    assert updated_row.completed_scan_version == 0


def test_register_archives_bulk_marks_absence_only_in_scanned_directories(tmp_path):
    db_path = tmp_path / "archive_presence.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    db_setup.initialize_database(eng, reset_tables=True)

    dir_a = tmp_path / "a" / "2024" / "01"
    dir_b = tmp_path / "b" / "2024" / "01"
    dir_a.mkdir(parents=True, exist_ok=True)
    dir_b.mkdir(parents=True, exist_ok=True)

    zip_a1 = dir_a / "169871_20240101_000000_000_TransportRSM.fpkg.e2d.zip"
    zip_a2 = dir_a / "169871_20240101_000001_000_TransportRSM.fpkg.e2d.zip"
    zip_b1 = dir_b / "169872_20240101_000000_000_TransportRSM.fpkg.e2d.zip"
    for path, payload in ((zip_a1, "a1"), (zip_a2, "a2"), (zip_b1, "b1")):
        path.write_text(payload, encoding="utf-8")

    db_setup.register_archives_bulk(
        eng,
        [zip_a1, zip_a2, zip_b1],
        scanned_directory_paths=[dir_a, dir_b],
    )
    db_setup.register_archives_bulk(
        eng,
        [zip_a1],
        scanned_directory_paths=[dir_a],
    )

    with eng.connect() as conn:
        rows = conn.execute(
            sa.select(ArchiveMetadata.source_path, ArchiveMetadata.is_present)
        ).fetchall()

    presence = {row.source_path: row.is_present for row in rows}
    assert presence[str(zip_a1)] is True
    assert presence[str(zip_a2)] is False
    assert presence[str(zip_b1)] is True


def test_old_parser_version_marks_artifact_pending(
    run_repo_python, tmp_path
):
    sqlite_path = tmp_path / "state.sqlite3"
    zip_path = tmp_path / "169871_20231107_024218_025_TransportRSM.fpkg.e2d.zip"

    script = """
import json
import os
from pathlib import Path
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import (
    ArchiveMetadata,
    ArtifactManifest,
    FileHashRegistry,
    FileMetadata,
)
from e2ude_core.db.setup import initialize_database, register_archives_bulk
from e2ude_core.orchestration.state import plan_archive_run

zip_path = Path(os.environ["ZIP_PATH"])

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
archive_id = register_archives_bulk(eng, [zip_path])[zip_path]

with eng.begin() as conn:
    hash_result = conn.execute(
        FileHashRegistry.__table__.insert().values(md5=b"1234567890abcdef")
    )
    hash_id = hash_result.inserted_primary_key[0]

    conn.execute(
        FileMetadata.__table__.insert().values(
            archive_id=archive_id,
            hash_id=hash_id,
            relative_path="sample_Segments",
            file_type="SEGMENTS",
            file_size_bytes=1,
        )
    )

    conn.execute(
        ArtifactManifest.__table__.insert().values(
            hash_id=hash_id,
            target_table="rsmdata_segments",
            parser_version=0,
        )
    )

    conn.execute(
        ArchiveMetadata.__table__.update()
        .where(ArchiveMetadata.id == archive_id)
        .values(
            completed_scan_version=1,
            required_scan_version=1,
        )
    )

plan = plan_archive_run(eng, archive_id)
missing_items = [
    [item.hash_id, model.__tablename__]
    for item in plan.work_items
    for model in item.target_models
]

print(
    json.dumps(
        {
            "needs_scan": plan.needs_scan,
            "missing_items": missing_items,
            "work_items": [
                {
                    "relative_path": item.relative_path,
                    "file_type": item.file_type.value,
                    "target_tables": [model.__tablename__ for model in item.target_models],
                }
                for item in plan.work_items
            ],
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
            "ZIP_PATH": zip_path,
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload["needs_scan"] is False
    assert payload["missing_items"]
    assert payload["work_items"] == [
        {
            "relative_path": "sample_Segments",
            "file_type": "SEGMENTS",
            "target_tables": ["rsmdata_segments"],
        }
    ]


def test_process_staged_archive_records_single_metadata_scan_job(
    run_repo_python, tmp_path
):
    sqlite_path = tmp_path / "workflow.sqlite3"
    staged_dir = tmp_path / "staged"
    zip_path = tmp_path / "123456_20240101_000000_000_TransportRSM.fpkg.e2d.zip"

    script = """
import json
import os
from pathlib import Path
from sqlalchemy import select
from e2ude_core.config import settings
from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob
from e2ude_core.db.setup import initialize_database, register_archives_bulk
from e2ude_core.orchestration.workflow import process_staged_archive

staged_dir = Path(os.environ["STAGED_DIR"])
staged_dir.mkdir(parents=True, exist_ok=True)
(staged_dir / "123456_20240101_000000_000_Segments").write_text(
    (
        "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,"
        "01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,"
        "false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,"
        "false,0,0,,,"
    ),
    encoding="utf-8",
)
zip_path = Path(os.environ["ZIP_PATH"])

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
archive_id = register_archives_bulk(eng, [zip_path])[zip_path]
result = process_staged_archive(eng, archive_id, staged_dir, EtlContext.capture())

with eng.connect() as conn:
    rows = conn.execute(
        select(ProcessingJob.parser_id, ProcessingJob.target_table).where(
            ProcessingJob.parser_id == "MetadataScanHandler"
        )
    ).fetchall()

print(
    json.dumps(
        {
            "count": len(rows),
            "targets": [row.target_table for row in rows],
            "rows_uploaded": result.rows_uploaded,
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
            "STAGED_DIR": staged_dir,
            "ZIP_PATH": zip_path,
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload["count"] == 1
    assert payload["targets"] == ["metadata_file"]
    assert payload["rows_uploaded"] > 0


def test_initialize_database_only_creates_runtime_tables(run_repo_python, tmp_path):
    sqlite_path = tmp_path / "runtime_tables.sqlite3"

    script = """
import json
from sqlalchemy import inspect
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.setup import initialize_database

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)

inspector = inspect(eng)
tables = sorted(inspector.get_table_names())

print(json.dumps({"tables": tables}))
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_DATABASE__TYPE": "sqlite3",
            "E2UDE_DATABASE__DB_LOCATION": sqlite_path,
            "E2UDE_DATABASE__IN_MEMORY": "false",
        },
    )
    payload = json.loads(result.stdout.strip())

    assert "metadata_archive" in payload["tables"]
    assert "metadata_file" in payload["tables"]
    assert "metadata_hash_registry" in payload["tables"]
    assert "processing_sessions" in payload["tables"]
    assert "processing_jobs" in payload["tables"]
    assert "metadata_artifact_manifest" in payload["tables"]
    assert "rsmdata_tmptr" in payload["tables"]
    assert "rsmdata_segments" in payload["tables"]
    assert "rsmdata_mc_pfc_db" in payload["tables"]
    assert "rsmdata_mc_gfc_db" not in payload["tables"]


def test_initialize_database_rejects_existing_runtime_tables_missing_columns(tmp_path):
    db_path = tmp_path / "old_schema.sqlite3"
    eng = sa.create_engine(f"sqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            sa.text(
                """
                CREATE TABLE processing_sessions (
                    id INTEGER PRIMARY KEY,
                    folder_id INTEGER NOT NULL,
                    git_hash VARCHAR(40),
                    user_name VARCHAR(40),
                    status VARCHAR(20),
                    start_time DATETIME,
                    end_time DATETIME
                )
                """
            )
        )

    with pytest.raises(RuntimeError, match="Database schema is out of date") as exc:
        db_setup.initialize_database(eng, reset_tables=False)

    message = str(exc.value)
    assert "processing_sessions.host_name" in message


def test_job_mark_running_preserves_original_start_time(run_repo_python, tmp_path):
    sqlite_path = tmp_path / "job_manager.sqlite3"
    zip_path = tmp_path / "123456_20240101_000000_000_TransportRSM.fpkg.e2d.zip"

    script = """
import json
import os
import time
from pathlib import Path
from sqlalchemy import select
from e2ude_core.config import settings
from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob
from e2ude_core.db.setup import initialize_database, register_archives_bulk
from e2ude_core.orchestration.runs import (
    create_processing_job,
    create_processing_session,
    finalize_processing_session,
    mark_processing_job_completed,
    mark_processing_job_running,
)

zip_path = Path(os.environ["ZIP_PATH"])
eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
archive_id = register_archives_bulk(eng, [zip_path])[zip_path]

session_id = create_processing_session(eng, EtlContext.capture())
job_id = create_processing_job(
    eng,
    session_id,
    archive_id=archive_id,
    file_type="SEGMENTS",
    parser_id="segments",
    target_table="rsmdata_segments",
    parser_version=1,
)

mark_processing_job_running(eng, job_id, "first progress")
with eng.connect() as conn:
    first_start = conn.execute(
        select(ProcessingJob.start_time).where(ProcessingJob.id == job_id)
    ).scalar_one()

time.sleep(1.2)
mark_processing_job_running(eng, job_id, "second progress")
with eng.connect() as conn:
    row = conn.execute(
        select(ProcessingJob.start_time, ProcessingJob.message).where(
            ProcessingJob.id == job_id
        )
    ).one()

mark_processing_job_completed(eng, job_id, "done", rows_uploaded=0)
finalize_processing_session(eng, session_id)

print(
    json.dumps(
        {
            "first_start": str(first_start),
            "second_start": str(row.start_time),
            "message": row.message,
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
            "ZIP_PATH": zip_path,
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload["first_start"] == payload["second_start"]
    assert payload["message"] == "second progress"


def test_run_processing_job_persists_explicit_result_and_target_key(
    run_repo_python, tmp_path
):
    sqlite_path = tmp_path / "run_job.sqlite3"
    zip_path = tmp_path / "123456_20240101_000000_000_TransportRSM.fpkg.e2d.zip"

    script = """
import json
import os
from pathlib import Path
from sqlalchemy import select
from e2ude_core.config import settings
from e2ude_core.context import EtlContext
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob
from e2ude_core.db.setup import initialize_database, register_archives_bulk
from e2ude_core.orchestration.runs import (
    create_processing_session,
    finalize_processing_session,
    JobRunResult,
    run_processing_job,
)

zip_path = Path(os.environ["ZIP_PATH"])
eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
archive_id = register_archives_bulk(eng, [zip_path])[zip_path]

session_id = create_processing_session(eng, EtlContext.capture())
run_processing_job(
    eng,
    session_id,
    lambda _report_progress: JobRunResult(
        rows_uploaded=7,
        completion_message="custom completion",
    ),
    archive_id=archive_id,
    file_type="MCDATA",
    parser_id="mcdata",
    target_table="rsmdata_mc_pfc_db",
    parser_version=3,
)
finalize_processing_session(eng, session_id)

with eng.connect() as conn:
    row = conn.execute(
        select(
            ProcessingJob.parser_id,
            ProcessingJob.target_table,
            ProcessingJob.parser_version,
            ProcessingJob.rows_uploaded,
            ProcessingJob.message,
            ProcessingJob.status,
        )
    ).one()

print(
    json.dumps(
        {
            "parser_id": row.parser_id,
            "target_table": row.target_table,
            "parser_version": row.parser_version,
            "rows_uploaded": row.rows_uploaded,
            "message": row.message,
            "status": row.status.value,
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
            "ZIP_PATH": zip_path,
        },
    )
    payload = json.loads(result.stdout.strip())

    assert payload == {
        "parser_id": "mcdata",
        "target_table": "rsmdata_mc_pfc_db",
        "parser_version": 3,
        "rows_uploaded": 7,
        "message": "custom completion",
        "status": "COMPLETED",
    }


def test_main_culls_stale_runs_before_discovery(
    run_repo_python, run_repo_command, write_app_config, tmp_path
):
    sqlite_path = tmp_path / "stale_cull.sqlite3"
    scan_root = tmp_path / "scan_root"
    staging_root = tmp_path / "staging"
    stale_zip = scan_root / "123456_20240101_000000_000_TransportRSM.fpkg.e2d.zip"
    fresh_zip = scan_root / "123457_20240101_000000_000_TransportRSM.fpkg.e2d.zip"
    scan_root.mkdir(parents=True, exist_ok=True)
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

    script = """
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import select
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob, ProcessingSession, StatusEnum
from e2ude_core.db.setup import initialize_database, register_archives_bulk

stale_zip = Path(os.environ["STALE_ZIP"])
fresh_zip = Path(os.environ["FRESH_ZIP"])

eng = get_engine(settings.database)
initialize_database(eng, reset_tables=True)
archive_map = register_archives_bulk(eng, [stale_zip, fresh_zip])
stale_start = datetime.utcnow() - timedelta(days=2)
fresh_start = datetime.utcnow() - timedelta(hours=1)

with eng.begin() as conn:
    stale_session = conn.execute(
        ProcessingSession.__table__.insert().values(
            status=StatusEnum.RUNNING,
            start_time=stale_start,
        )
    ).inserted_primary_key[0]
    fresh_session = conn.execute(
        ProcessingSession.__table__.insert().values(
            status=StatusEnum.RUNNING,
            start_time=fresh_start,
        )
    ).inserted_primary_key[0]

    stale_running_job = conn.execute(
        ProcessingJob.__table__.insert().values(
            session_id=stale_session,
            archive_id=archive_map[stale_zip],
            parser_id="segments",
            target_table="rsmdata_segments",
            status=StatusEnum.RUNNING,
            start_time=stale_start,
        )
    ).inserted_primary_key[0]
    stale_pending_job = conn.execute(
        ProcessingJob.__table__.insert().values(
            session_id=stale_session,
            archive_id=archive_map[stale_zip],
            parser_id="mcdata",
            target_table="rsmdata_mc_pfc_db",
            status=StatusEnum.PENDING,
        )
    ).inserted_primary_key[0]
    fresh_running_job = conn.execute(
        ProcessingJob.__table__.insert().values(
            session_id=fresh_session,
            archive_id=archive_map[fresh_zip],
            parser_id="segments",
            target_table="rsmdata_segments",
            status=StatusEnum.RUNNING,
            start_time=fresh_start,
        )
    ).inserted_primary_key[0]

with eng.connect() as conn:
    session_rows = conn.execute(
        select(ProcessingSession.id, ProcessingSession.status, ProcessingSession.end_time)
        .order_by(ProcessingSession.id)
    ).fetchall()
    job_rows = conn.execute(
        select(
            ProcessingJob.id,
            ProcessingJob.parser_id,
            ProcessingJob.status,
            ProcessingJob.message,
            ProcessingJob.end_time,
        ).order_by(ProcessingJob.id)
    ).fetchall()

print(
    json.dumps(
        {
            "sessions": [
                {
                    "id": row.id,
                    "status": row.status.value,
                    "end_time": None if row.end_time is None else str(row.end_time),
                }
                for row in session_rows
            ],
            "jobs": [
                {
                    "id": row.id,
                    "parser_id": row.parser_id,
                    "status": row.status.value,
                    "message": row.message,
                    "end_time": None if row.end_time is None else str(row.end_time),
                }
                for row in job_rows
            ],
            "ids": {
                "stale_running_job": stale_running_job,
                "stale_pending_job": stale_pending_job,
                "fresh_running_job": fresh_running_job,
                "stale_session": stale_session,
                "fresh_session": fresh_session,
            },
        }
    )
)
"""

    result = run_repo_python(
        script,
        {
            "E2UDE_CONFIG_PATH": config_path,
            "STALE_ZIP": stale_zip,
            "FRESH_ZIP": fresh_zip,
        },
    )
    assert json.loads(result.stdout.strip())["ids"]["stale_session"] > 0

    run_repo_command(
        [".\\.venv\\Scripts\\python.exe", "-m", "e2ude_core.main"],
        {
            "E2UDE_CONFIG_PATH": config_path,
            "E2UDE_RUNTIME__DISCOVERY_WORKERS": 2,
            "E2UDE_RUNTIME__UNZIP_WORKERS": 1,
            "E2UDE_RUNTIME__PROCESS_WORKERS": 1,
            "E2UDE_RUNTIME__PIPELINE_BUFFER_SIZE": 1,
        },
    )

    payload = json.loads(
        run_repo_python(
            """
import json
import os
from pathlib import Path
from sqlalchemy import select
from e2ude_core.config import settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ProcessingJob, ProcessingSession

eng = get_engine(settings.database)
with eng.connect() as conn:
    session_rows = conn.execute(
        select(ProcessingSession.id, ProcessingSession.status, ProcessingSession.end_time)
        .order_by(ProcessingSession.id)
    ).fetchall()
    job_rows = conn.execute(
        select(
            ProcessingJob.id,
            ProcessingJob.parser_id,
            ProcessingJob.status,
            ProcessingJob.message,
            ProcessingJob.end_time,
        ).order_by(ProcessingJob.id)
    ).fetchall()

print(
    json.dumps(
        {
            "sessions": [
                {
                    "id": row.id,
                    "status": row.status.value,
                    "end_time": None if row.end_time is None else str(row.end_time),
                }
                for row in session_rows
            ],
            "jobs": [
                {
                    "id": row.id,
                    "parser_id": row.parser_id,
                    "status": row.status.value,
                    "message": row.message,
                    "end_time": None if row.end_time is None else str(row.end_time),
                }
                for row in job_rows
            ],
        }
    )
)
""",
            {
                "E2UDE_CONFIG_PATH": config_path,
            },
        ).stdout.strip()
    )

    sessions_by_id = {row["id"]: row for row in payload["sessions"]}
    jobs_by_id = {row["id"]: row for row in payload["jobs"]}
    ids = json.loads(result.stdout.strip())["ids"]

    assert sessions_by_id[ids["stale_session"]]["status"] == "ERROR"
    assert sessions_by_id[ids["stale_session"]]["end_time"] is not None
    assert sessions_by_id[ids["fresh_session"]]["status"] == "RUNNING"

    assert jobs_by_id[ids["stale_running_job"]]["status"] == "ERROR"
    assert jobs_by_id[ids["stale_running_job"]]["end_time"] is not None
    assert "stale" in jobs_by_id[ids["stale_running_job"]]["message"].lower()

    assert jobs_by_id[ids["stale_pending_job"]]["status"] == "ERROR"
    assert jobs_by_id[ids["stale_pending_job"]]["end_time"] is not None
    assert "stale" in jobs_by_id[ids["stale_pending_job"]]["message"].lower()

    assert jobs_by_id[ids["fresh_running_job"]]["status"] == "RUNNING"
    assert jobs_by_id[ids["fresh_running_job"]]["end_time"] is None
