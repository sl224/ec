from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from e2ude_core.config import settings
from e2ude_core.services.discovery import discover_archives


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure archive discovery against a source tree."
    )
    parser.add_argument(
        "--scan-root",
        type=Path,
        default=settings.paths.scan_root,
        help="Source tree to scan. Defaults to configured scan_root.",
    )
    args = parser.parse_args()

    scan_root = args.scan_root
    if scan_root is None:
        raise SystemExit("scan_root is not configured")
    if not scan_root.exists():
        raise SystemExit(f"scan_root does not exist: {scan_root}")

    started_at = time.perf_counter()
    result = discover_archives(
        scan_root,
        max_workers=settings.runtime.discovery_workers,
    )
    duration_seconds = time.perf_counter() - started_at

    print(
        json.dumps(
            {
                "scan_root": str(scan_root),
                "duration_seconds": round(duration_seconds, 3),
                "archives": len(result.archives),
                "scanned_directories": result.scanned_directory_count,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
