import logging
import shutil
import tempfile
from pathlib import Path
from typing import Sequence, Union
from zipfile import ZipFile

logger = logging.getLogger(__name__)


def _matches_any(path_str: str, patterns: Sequence[str]) -> bool:
    candidate = Path(path_str)
    return any(candidate.match(pattern) for pattern in patterns)


def _select_members(
    zip_ref: ZipFile,
    active_patterns: Sequence[str] | None,
    container_pattern: str,
    *,
    nested_prefix: str = "",
) -> list[str]:
    names = zip_ref.namelist()
    if active_patterns is None:
        return names
    return [
        name
        for name in names
        if _matches_any(
            f"{nested_prefix}/{name}" if nested_prefix else name,
            active_patterns,
        )
        or Path(name).match(container_pattern)
    ]


def extract_transport_zip(
    zip_path: Path,
    extract_dir: Path,
    *,
    active_patterns: Sequence[str] | None = None,
    max_nesting: int = 3,
) -> None:
    """Extract all files or only active files plus required nested containers."""
    pending: list[tuple[Path, Path, str]] = [(zip_path, extract_dir, "")]
    processed = 0
    container_pattern = "*.zip" if active_patterns is None else "*RSM_RawArchive.zip"

    while pending:
        if processed >= max_nesting:
            raise RuntimeError("ERROR: hit max nesting limit")

        current_zip, current_dir, nested_prefix = pending.pop()
        current_dir.mkdir(parents=True, exist_ok=True)
        with ZipFile(current_zip, "r") as zip_ref:
            members = _select_members(
                zip_ref,
                active_patterns,
                container_pattern,
                nested_prefix=nested_prefix,
            )
            if members:
                zip_ref.extractall(current_dir, members=members)

        if current_zip != zip_path:
            current_zip.unlink()

        for nested_zip in current_dir.rglob(container_pattern):
            nested_root = nested_zip.with_suffix("")
            pending.append((nested_zip, nested_root, nested_root.name))
        processed += 1


class UnzipContext:
    """Extract a transport zip to a temporary directory."""

    def __init__(self, zip_path: Union[str, Path]):
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(f"Zip file not found: {self.zip_path}")

        self.temp_dir: Path | None = None

    def __enter__(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        logger.info("Extracting '%s' to %s", self.zip_path.name, self.temp_dir)

        extract_transport_zip(self.zip_path, self.temp_dir)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.temp_dir and Path(self.temp_dir).exists():
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as exc:
                logger.error("Failed to delete temp dir %s: %s", self.temp_dir, exc)
