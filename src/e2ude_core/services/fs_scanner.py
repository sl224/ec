import hashlib
from pathlib import Path
import logging
from typing import List, Dict, Any
from enum import StrEnum

logger = logging.getLogger(__name__)


def calculate_md5(file_path: Path, chunk_size=4096) -> str:
    """
    Calculates the MD5 hash of a file.
    """
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
    except Exception as e:
        logger.error(f"Failed to calculate MD5 for {file_path}: {e}")
        return "error"
    return hash_md5.hexdigest()


def scan_directory(
    extract_dir: Path, pattern_to_type_map: Dict[str, StrEnum], unknown_type: StrEnum
) -> List[Dict[str, Any]]:
    """
    Scans a directory, calculates hashes, and classifies files.

    This is a pure function that only interacts with the filesystem.

    Args:
        extract_dir: The root directory to scan recursively.
        pattern_to_type_map: A dict mapping glob patterns to FileType enums.
        unknown_type: The enum value to use if no pattern matches.

    Returns:
        A list of dictionaries, where each dict contains raw file info:
        [
            {
                "relative_path": "...",
                "file_type": "...",
                "file_size_bytes": 1234,
                "md5": "..."
            },
            ...
        ]
    """
    logger.info(f"Scanning directory {extract_dir} for files...")

    files_to_insert = []
    all_files = [p for p in extract_dir.rglob("*") if p.is_file()]

    if not all_files:
        logger.warning(f"No files found in {extract_dir}.")
        return []

    for file_path in all_files:
        rel_path = file_path.relative_to(extract_dir)

        # Find file type
        file_type = unknown_type
        for pattern, ftype in pattern_to_type_map.items():
            if rel_path.match(pattern):
                file_type = ftype
                break

        file_hash = calculate_md5(file_path)
        if file_hash == "error":
            logger.warning(f"Could not hash {str(rel_path)}, skipping.")
            continue

        files_to_insert.append(
            {
                "relative_path": str(rel_path),
                "file_type": file_type.value,  # Store the string value
                "file_size_bytes": file_path.stat().st_size,
                "md5": file_hash,
            }
        )

    logger.info(f"Scan complete. Found and hashed {len(files_to_insert)} files.")
    return files_to_insert
