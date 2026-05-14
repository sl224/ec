from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_seed_script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "seed_legacy_schema.py"
    spec = importlib.util.spec_from_file_location("seed_legacy_schema", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_path_keys_use_windows_path_semantics_without_filesystem_access():
    seed = _load_seed_script()

    assert seed._path_key(
        r" \\RSINY1-ILSFS/RSM/169879/2025/11/archive.zip "
    ) == seed._path_key(r"\\rsiny1-ilsfs\RSM\169879\2025\11\archive.zip")
    assert (
        seed._archive_name_key(r"\\RSINY1-ILSFS\RSM\169879\2025\11\Archive.ZIP")
        == "archive.zip"
    )


def test_folder_mapping_matches_normalized_exact_paths():
    seed = _load_seed_script()

    counts, folder_archive_ids, examples = seed._folder_mapping_from_rows(
        file_counts={10: (3, 1)},
        folder_paths={10: r"\\SERVER/share/rsm/archive.zip "},
        archive_paths={99: r"\\server\share\rsm\archive.zip"},
        method="exact-path",
    )

    assert counts == {
        "total_files": 3,
        "missing_folder": 0,
        "missing_archive": 0,
        "ambiguous_archive": 0,
    }
    assert folder_archive_ids == {10: 99}
    assert examples == []


def test_folder_mapping_reports_missing_and_ambiguous_archive_names():
    seed = _load_seed_script()

    counts, folder_archive_ids, examples = seed._folder_mapping_from_rows(
        file_counts={10: (3, 1), 20: (5, 4)},
        folder_paths={10: r"\\server\share\missing.zip", 20: r"D:\x\dupe.zip"},
        archive_paths={
            99: r"\\server\share\a\dupe.zip",
            100: r"\\server\share\b\dupe.zip",
        },
        method="archive-name",
    )

    assert counts == {
        "total_files": 8,
        "missing_folder": 0,
        "missing_archive": 3,
        "ambiguous_archive": 5,
    }
    assert folder_archive_ids == {}
    assert examples == [
        [1, 10, r"\\server\share\missing.zip", "missing.zip", 0],
        [4, 20, r"D:\x\dupe.zip", "dupe.zip", 2],
    ]


def test_legacy_folder_mapping_derives_current_archive_rows():
    seed = _load_seed_script()

    counts, folder_archive_ids, examples, archive_rows = (
        seed._legacy_folder_mapping_from_rows(
            file_counts={24145: (66, 1)},
            folder_paths={
                24145: (
                    r"\\rsiny1-ilsfs\RSM\169879\2025\11"
                    r"\169879_20251105_024048_075_TransportRSM.fpkg.e2d.zip"
                )
            },
            scanner_version=3,
            handler_generation="abc123",
        )
    )

    assert counts == {
        "total_files": 66,
        "missing_folder": 0,
        "missing_archive": 0,
        "ambiguous_archive": 0,
    }
    assert folder_archive_ids == {24145: 24145}
    assert examples == []
    assert archive_rows[0]["id"] == 24145
    assert archive_rows[0]["buno"] == "169879"
    assert archive_rows[0]["source_size_bytes"] == 0
    assert archive_rows[0]["completed_scan_version"] == 3
    assert archive_rows[0]["required_handler_generation"] == "abc123"
    assert archive_rows[0]["completed_handler_generation"] is None
    assert archive_rows[0]["state"] == "NEEDS_PROCESSING"


def test_auto_folder_mapping_prefers_complete_exact_path():
    seed = _load_seed_script()
    counts = {
        "exact-path": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
        "archive-name": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
        "legacy-folder": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
    }

    assert seed._select_folder_mapping("auto", counts) == "exact-path"


def test_auto_folder_mapping_uses_unique_archive_name_when_exact_path_fails():
    seed = _load_seed_script()
    counts = {
        "exact-path": {
            "missing_folder": 0,
            "missing_archive": 10,
            "ambiguous_archive": 0,
        },
        "archive-name": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
        "legacy-folder": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
    }

    assert seed._select_folder_mapping("auto", counts) == "archive-name"


def test_forced_folder_mapping_is_not_overridden():
    seed = _load_seed_script()
    counts = {
        "exact-path": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
        "archive-name": {
            "missing_folder": 0,
            "missing_archive": 25,
            "ambiguous_archive": 5,
        },
        "legacy-folder": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
    }

    assert seed._select_folder_mapping("archive-name", counts) == "archive-name"


def test_auto_folder_mapping_falls_back_to_legacy_folder():
    seed = _load_seed_script()
    counts = {
        "exact-path": {
            "missing_folder": 0,
            "missing_archive": 10,
            "ambiguous_archive": 0,
        },
        "archive-name": {
            "missing_folder": 0,
            "missing_archive": 10,
            "ambiguous_archive": 0,
        },
        "legacy-folder": {
            "missing_folder": 0,
            "missing_archive": 0,
            "ambiguous_archive": 0,
        },
    }

    assert seed._select_folder_mapping("auto", counts) == "legacy-folder"
