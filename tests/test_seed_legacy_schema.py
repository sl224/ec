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
    }

    assert seed._select_folder_mapping("archive-name", counts) == "archive-name"
