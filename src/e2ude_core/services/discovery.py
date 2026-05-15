import logging
import os
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_DISCOVERY_PROGRESS_INTERVAL_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class DiscoveredArchive:
    path: Path
    size_bytes: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    archives: tuple[DiscoveredArchive, ...]
    scanned_directory_count: int


@dataclass(frozen=True, slots=True)
class _DirectoryListing:
    path: Path
    subdirectories: tuple[Path, ...]
    archives: tuple[DiscoveredArchive, ...]


def _rsm_zip_filter(entry: os.DirEntry) -> bool:
    return entry.name.lower().endswith("transportrsm.fpkg.e2d.zip")


def _normalize(path: Path | str) -> str:
    return str(Path(path))


def _scan_directory(path: Path) -> _DirectoryListing:
    subdirectories: list[Path] = []
    archives: list[DiscoveredArchive] = []

    with os.scandir(path) as it:
        for entry in it:
            if entry.is_dir(follow_symlinks=False):
                subdirectories.append(Path(entry.path))
            elif entry.is_file(follow_symlinks=False) and _rsm_zip_filter(entry):
                entry_stat = entry.stat(follow_symlinks=False)
                archives.append(
                    DiscoveredArchive(
                        path=Path(entry.path),
                        size_bytes=entry_stat.st_size,
                        mtime_ns=entry_stat.st_mtime_ns,
                    )
                )

    return _DirectoryListing(
        path=path,
        subdirectories=tuple(subdirectories),
        archives=tuple(archives),
    )


def discover_archives(
    search_path: Path,
    *,
    max_workers: int = 1024,
) -> DiscoveryResult:
    """Find all transport archive locators under search_path."""
    if not search_path.exists():
        raise ValueError(f"Search path does not exist: {search_path}")

    archives: list[DiscoveredArchive] = []
    scanned_directories: set[str] = set()
    scheduled_scan_paths = {_normalize(search_path)}
    started_at = time.perf_counter()
    next_progress_at = started_at + _DISCOVERY_PROGRESS_INTERVAL_SECONDS

    def log_progress(pending_dirs: int) -> None:
        nonlocal next_progress_at
        now = time.perf_counter()
        if now < next_progress_at:
            return

        elapsed_seconds = max(now - started_at, 0.001)
        scanned_count = len(scanned_directories)
        logger.info(
            "Discovery: elapsed=%.1fs scanned_dirs=%s archives=%s "
            "pending_dirs=%s rate=%.1f dirs/s",
            elapsed_seconds,
            scanned_count,
            len(archives),
            pending_dirs,
            scanned_count / elapsed_seconds,
        )
        next_progress_at = now + _DISCOVERY_PROGRESS_INTERVAL_SECONDS

    logger.info("Discovery: scanning root=%s zip_reads=none.", search_path)

    futures = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures[executor.submit(_scan_directory, search_path)] = _normalize(search_path)

        while futures:
            done, _ = wait(
                tuple(futures),
                timeout=_DISCOVERY_PROGRESS_INTERVAL_SECONDS,
                return_when=FIRST_COMPLETED,
            )
            if not done:
                log_progress(len(futures))
                continue

            for future in done:
                futures.pop(future, None)
                listing = future.result()

                normalized_path = _normalize(listing.path)
                scanned_directories.add(normalized_path)
                archives.extend(listing.archives)

                for child_path in listing.subdirectories:
                    normalized_child = _normalize(child_path)
                    if normalized_child in scheduled_scan_paths:
                        continue
                    scheduled_scan_paths.add(normalized_child)
                    futures[executor.submit(_scan_directory, child_path)] = (
                        normalized_child
                    )

            log_progress(len(futures))

    duration_seconds = max(time.perf_counter() - started_at, 0.001)
    logger.info(
        "Discovery complete: duration=%.1fs archives=%s scanned_dirs=%s "
        "rate=%.1f dirs/s",
        duration_seconds,
        len(archives),
        len(scanned_directories),
        len(scanned_directories) / duration_seconds,
    )
    return DiscoveryResult(
        archives=tuple(sorted(archives, key=lambda item: str(item.path))),
        scanned_directory_count=len(scanned_directories),
    )
