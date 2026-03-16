import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

from e2ude_core.runtime_files import CATALOG_FILE_PATTERNS, FileType
from e2ude_core.services.fs_engine import ParallelFileScanner

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class FileScanResult:
    relative_path: str
    file_type: FileType
    file_size_bytes: int
    md5: bytes


def calculate_stream_md5(f_obj, chunk_size: int = 65536) -> bytes:
    hash_md5 = hashlib.md5()
    while chunk := f_obj.read(chunk_size):
        hash_md5.update(chunk)
    return hash_md5.digest()


def detect_file_type(relative_path: Path | str) -> FileType:
    candidate = (
        relative_path if isinstance(relative_path, Path) else Path(relative_path)
    )
    for file_type, pattern in CATALOG_FILE_PATTERNS:
        if candidate.match(pattern):
            return file_type
    return FileType.UNKNOWN


def _process_staged_file(path: Path, root_dir: Path) -> FileScanResult:
    relative_path = path.relative_to(root_dir)
    file_type = detect_file_type(relative_path)

    try:
        with path.open("rb") as file_obj:
            md5 = calculate_stream_md5(file_obj)

        return FileScanResult(
            relative_path=str(relative_path),
            file_type=file_type,
            file_size_bytes=path.stat().st_size,
            md5=md5,
        )
    except OSError as exc:
        logger.error("Failed to hash staged file %s: %s", path, exc)
        return FileScanResult(
            relative_path=str(relative_path),
            file_type=FileType.UNKNOWN,
            file_size_bytes=0,
            md5=b"",
        )


def catalog_staged_folder(staged_root: Path) -> List[FileScanResult]:
    scanner = ParallelFileScanner(max_workers=16)
    return scanner.walk(
        root_path=staged_root,
        filter_func=lambda _entry: True,
        action_func=lambda path: _process_staged_file(path, staged_root),
        show_progress=False,
    )
