import logging
import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class DiscoveryMode(StrEnum):
    INCREMENTAL = "incremental"
    RECONCILE = "reconcile"


@dataclass(frozen=True, slots=True)
class DiscoveredArchive:
    path: Path
    size_bytes: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class KnownDiscoveryDirectory:
    path: Path
    mtime_ns: int
    contains_archives: bool


@dataclass(frozen=True, slots=True)
class DiscoveryDirectorySnapshot:
    path: Path
    mtime_ns: int
    contains_archives: bool
    scanned: bool


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    archives: tuple[DiscoveredArchive, ...]
    directory_snapshots: tuple[DiscoveryDirectorySnapshot, ...]
    scanned_directory_paths: tuple[Path, ...]
    missing_directory_paths: tuple[Path, ...]
    scanned_directory_count: int
    skipped_directory_count: int
    archive_directory_scan_count: int
    frontier_directory_scan_count: int


@dataclass(frozen=True, slots=True)
class _DirectoryListing:
    path: Path
    mtime_ns: int
    subdirectories: tuple[Tuple[Path, int], ...]
    archives: tuple[DiscoveredArchive, ...]


def _rsm_zip_filter(entry: os.DirEntry) -> bool:
    return entry.name.lower().endswith("transportrsm.fpkg.e2d.zip")


def _normalize(path: Path | str) -> str:
    return str(Path(path))


def _stat_directory(path: Path) -> tuple[str, bool, int]:
    try:
        return (_normalize(path), True, path.stat().st_mtime_ns)
    except OSError:
        return (_normalize(path), False, 0)


def _scan_directory(path: Path) -> _DirectoryListing:
    dir_mtime_ns = path.stat().st_mtime_ns
    subdirectories: list[Tuple[Path, int]] = []
    archives: list[DiscoveredArchive] = []

    with os.scandir(path) as it:
        for entry in it:
            if entry.is_dir(follow_symlinks=False):
                try:
                    entry_stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                subdirectories.append((Path(entry.path), entry_stat.st_mtime_ns))
            elif entry.is_file(follow_symlinks=False) and _rsm_zip_filter(entry):
                try:
                    entry_stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                archives.append(
                    DiscoveredArchive(
                        path=Path(entry.path),
                        size_bytes=entry_stat.st_size,
                        mtime_ns=entry_stat.st_mtime_ns,
                    )
                )

    return _DirectoryListing(
        path=path,
        mtime_ns=dir_mtime_ns,
        subdirectories=tuple(subdirectories),
        archives=tuple(archives),
    )


def discover_archives(
    search_path: Path,
    *,
    known_directory_states: Dict[str, KnownDiscoveryDirectory] | None = None,
    mode: DiscoveryMode | str = DiscoveryMode.INCREMENTAL,
    max_workers: int = 1024,
) -> DiscoveryResult:
    """
    Discover transport archives using safe source signals.

    Incremental mode relists directories known to contain archives on every run
    so in-place archive edits cannot be missed. Directory mtimes are used only
    as membership-change signals for non-archive frontier directories to
    discover new or missing subtrees more cheaply.
    """
    if not search_path.exists():
        raise ValueError(f"Search path does not exist: {search_path}")

    discovery_mode = mode if isinstance(mode, DiscoveryMode) else DiscoveryMode(mode)
    known_directory_states = known_directory_states or {}
    known_archive_directories = {
        path
        for path, state in known_directory_states.items()
        if state.contains_archives
    }
    archives: list[DiscoveredArchive] = []
    directory_snapshots: dict[str, DiscoveryDirectorySnapshot] = {}
    missing_directories: set[str] = set()
    scanned_directories: set[str] = set()
    skipped_directory_count = 0
    archive_directory_scan_count = 0
    frontier_directory_scan_count = 0

    logger.info(
        "Scanning %s for RSM zips (mode=%s)...",
        search_path,
        discovery_mode.value,
    )
    if discovery_mode == DiscoveryMode.INCREMENTAL and known_directory_states:
        logger.info(
            "Incremental discovery is relisting %s known archive directories and "
            "using directory membership checks for the remaining frontier.",
            len(known_archive_directories),
        )

    seed_paths: list[Path] = []
    if discovery_mode == DiscoveryMode.RECONCILE or not known_directory_states:
        seed_paths = [search_path]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as stat_pool:
            future_map = {
                stat_pool.submit(_stat_directory, state.path): state
                for state in known_directory_states.values()
                if not state.contains_archives
            }
            for future in as_completed(future_map):
                state = future_map[future]
                path_str, exists, current_mtime_ns = future.result()
                if not exists:
                    missing_directories.add(path_str)
                    continue

                if current_mtime_ns != state.mtime_ns or state.path == search_path:
                    seed_paths.append(state.path)
                    continue

                directory_snapshots[path_str] = DiscoveryDirectorySnapshot(
                    path=state.path,
                    mtime_ns=current_mtime_ns,
                    contains_archives=False,
                    scanned=False,
                )
                skipped_directory_count += 1

        seed_paths.extend(
            Path(path)
            for path in sorted(known_archive_directories)
            if Path(path) != search_path
        )
        if search_path not in seed_paths:
            seed_paths.append(search_path)

    seed_paths = sorted(dict.fromkeys(seed_paths), key=str)

    futures = {}
    scheduled_scan_paths = {_normalize(path) for path in seed_paths}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for path in seed_paths:
            futures[executor.submit(_scan_directory, path)] = _normalize(path)

        while futures:
            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                futures.pop(future, None)
                try:
                    listing = future.result()
                except OSError as exc:
                    logger.debug("Directory scan failed: %s", exc)
                    continue

                normalized_path = _normalize(listing.path)
                directory_snapshots[normalized_path] = DiscoveryDirectorySnapshot(
                    path=listing.path,
                    mtime_ns=listing.mtime_ns,
                    contains_archives=bool(listing.archives),
                    scanned=True,
                )
                scanned_directories.add(normalized_path)
                archives.extend(listing.archives)
                if normalized_path in known_archive_directories:
                    archive_directory_scan_count += 1
                else:
                    frontier_directory_scan_count += 1

                for child_path, child_mtime_ns in listing.subdirectories:
                    normalized_child = _normalize(child_path)
                    if normalized_child in scheduled_scan_paths:
                        continue

                    child_state = known_directory_states.get(normalized_child)
                    if discovery_mode == DiscoveryMode.RECONCILE:
                        scheduled_scan_paths.add(normalized_child)
                        futures[executor.submit(_scan_directory, child_path)] = (
                            normalized_child
                        )
                        continue

                    if child_state is None or child_state.contains_archives:
                        scheduled_scan_paths.add(normalized_child)
                        futures[executor.submit(_scan_directory, child_path)] = (
                            normalized_child
                        )
                        continue

    if known_directory_states:
        missing_directories.update(
            set(known_directory_states) - set(directory_snapshots.keys())
        )

    scanned_directory_paths = tuple(
        sorted((Path(path) for path in scanned_directories), key=str)
    )
    missing_directory_paths = tuple(
        sorted((Path(path) for path in missing_directories), key=str)
    )
    snapshot_values = tuple(
        sorted(directory_snapshots.values(), key=lambda item: str(item.path))
    )

    logger.info(
        "Discovery complete. found=%s scanned_dirs=%s archive_dir_scans=%s "
        "frontier_dir_scans=%s skipped_dirs=%s missing_dirs=%s",
        len(archives),
        len(scanned_directory_paths),
        archive_directory_scan_count,
        frontier_directory_scan_count,
        skipped_directory_count,
        len(missing_directory_paths),
    )
    return DiscoveryResult(
        archives=tuple(sorted(archives, key=lambda item: str(item.path))),
        directory_snapshots=snapshot_values,
        scanned_directory_paths=scanned_directory_paths,
        missing_directory_paths=missing_directory_paths,
        scanned_directory_count=len(scanned_directory_paths),
        skipped_directory_count=skipped_directory_count,
        archive_directory_scan_count=archive_directory_scan_count,
        frontier_directory_scan_count=frontier_directory_scan_count,
    )
