from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

import sqlalchemy as sa

from e2ude_core.config import SQLiteConfig, settings
from e2ude_core.db.access import get_engine
from e2ude_core.db.models import ArchiveMetadata
from e2ude_core.db.setup import (
    initialize_database,
    load_directory_scan_cache,
    record_directory_snapshots,
    register_archives_bulk,
)
from e2ude_core.services.discovery import DiscoveryMode, discover_archives


def _load_archive_source_facts(
    eng: sa.Engine,
) -> dict[str, tuple[int, int, bool]]:
    with eng.connect() as conn:
        rows = conn.execute(
            sa.select(
                ArchiveMetadata.source_path,
                ArchiveMetadata.source_size_bytes,
                ArchiveMetadata.source_mtime_ns,
                ArchiveMetadata.is_present,
            )
        ).fetchall()
    return {
        row.source_path: (
            row.source_size_bytes,
            row.source_mtime_ns,
            row.is_present,
        )
        for row in rows
    }


def _run_discovery_pass(
    eng: sa.Engine,
    scan_root: Path,
    mode: DiscoveryMode,
    previous_source_facts: dict[str, tuple[int, int, bool]] | None = None,
) -> dict[str, int | float | str]:
    started_at = time.perf_counter()
    result = discover_archives(
        scan_root,
        known_directory_states=load_directory_scan_cache(eng, root_path=scan_root),
        mode=mode,
        max_workers=settings.runtime.discovery_workers,
    )
    record_directory_snapshots(
        eng,
        result.directory_snapshots,
        missing_paths=result.missing_directory_paths,
    )
    register_archives_bulk(
        eng,
        list(result.archives),
        scanned_directory_paths=result.scanned_directory_paths,
        missing_directory_paths=result.missing_directory_paths,
    )
    duration_seconds = time.perf_counter() - started_at
    current_source_facts = _load_archive_source_facts(eng)
    baseline_source_facts = previous_source_facts or {}
    changed_archive_count = sum(
        1
        for path, current in current_source_facts.items()
        if baseline_source_facts.get(path) != current
    ) + sum(1 for path in baseline_source_facts if path not in current_source_facts)
    return {
        "mode": mode.value,
        "duration_seconds": round(duration_seconds, 3),
        "archives_enumerated": len(result.archives),
        "archives_changed": changed_archive_count,
        "scanned_directories": result.scanned_directory_count,
        "archive_directory_scans": result.archive_directory_scan_count,
        "frontier_directory_scans": result.frontier_directory_scan_count,
        "skipped_directories": result.skipped_directory_count,
        "missing_directories": len(result.missing_directory_paths),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure discovery behavior against a source tree."
    )
    parser.add_argument(
        "--scan-root",
        type=Path,
        default=settings.paths.scan_root,
        help="Source tree to scan. Defaults to configured scan_root.",
    )
    parser.add_argument(
        "--sqlite-db",
        type=Path,
        help="Optional sqlite database path for persisted measurement state.",
    )
    args = parser.parse_args()

    scan_root = args.scan_root
    if scan_root is None:
        raise SystemExit("scan_root is not configured")
    if not scan_root.exists():
        raise SystemExit(f"scan_root does not exist: {scan_root}")

    sqlite_db = args.sqlite_db
    if sqlite_db is None:
        sqlite_db = Path(tempfile.gettempdir()) / "e2ude_discovery_measure.sqlite3"
    sqlite_db.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_db.exists():
        sqlite_db.unlink()

    eng = get_engine(
        SQLiteConfig(
            db_location=str(sqlite_db),
            in_memory=False,
        )
    )

    try:
        initialize_database(eng, reset_tables=True)
        reconcile_metrics = _run_discovery_pass(eng, scan_root, DiscoveryMode.RECONCILE)
        baseline_source_facts = _load_archive_source_facts(eng)
        incremental_metrics = _run_discovery_pass(
            eng,
            scan_root,
            DiscoveryMode.INCREMENTAL,
            previous_source_facts=baseline_source_facts,
        )
    finally:
        eng.dispose()

    print(
        json.dumps(
            {
                "scan_root": str(scan_root),
                "sqlite_db": str(sqlite_db),
                "reconcile": reconcile_metrics,
                "incremental": incremental_metrics,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
