import logging
import os
from pathlib import Path
from typing import List

from e2ude_core.services.fs_engine import ParallelFileScanner

logger = logging.getLogger(__name__)


def _rsm_zip_filter(entry: os.DirEntry) -> bool:
    """Predicate for Network Discovery Phase"""
    return entry.name.lower().endswith("transportrsm.fpkg.e2d.zip")


def _identity_action(path: Path) -> Path:
    """Action for Network Discovery (Just return the path)"""
    return path


def discover_network_zips(search_path: Path, max_workers: int = 1024) -> List[Path]:
    """
    Scans a network location for RSM zips using the generic parallel engine.
    High concurrency masks network latency.
    """
    if not search_path.exists():
        raise ValueError(f"Search path does not exist: {search_path}")

    logger.info(f"Scanning {search_path} for RSM zips...")

    scanner = ParallelFileScanner(max_workers=max_workers)
    zips = scanner.walk(
        root_path=search_path,
        filter_func=_rsm_zip_filter,
        action_func=_identity_action,
    )

    logger.info(f"Discovery complete. Found {len(zips)} zips.")
    return zips
