import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile

from e2ude_core.runtime_files import normalize_member_path, path_matches_pattern

RAW_ARCHIVE_PATTERN = "*RSM_RawArchive.zip"


@dataclass(frozen=True, slots=True)
class ArchiveMember:
    relative_path: str
    file_size_bytes: int
    compressed_size_bytes: int
    crc32: int
    zip_depth: int


def _is_raw_archive(name: str) -> bool:
    return path_matches_pattern(name, RAW_ARCHIVE_PATTERN)


def _nested_prefix(name: str) -> str:
    return Path(name).with_suffix("").name


def _member_path(prefix: str, name: str) -> str:
    return normalize_member_path(f"{prefix}/{name}" if prefix else name)


def _copy_member_to_temp(zip_ref: ZipFile, name: str) -> Path:
    handle = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    temp_path = Path(handle.name)
    try:
        with handle, zip_ref.open(name) as source:
            shutil.copyfileobj(source, handle)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path


def iter_archive_members(
    zip_path: Path, *, max_nesting: int = 3
) -> list[ArchiveMember]:
    """Return leaf file metadata from a transport zip without extracting leaves."""
    members: list[ArchiveMember] = []
    pending: list[tuple[Path, str, int, bool]] = [(zip_path, "", 0, False)]

    while pending:
        current_zip, prefix, depth, delete_after = pending.pop()
        if depth >= max_nesting:
            raise RuntimeError("hit max zip nesting limit")

        try:
            with ZipFile(current_zip, "r") as zip_ref:
                for info in zip_ref.infolist():
                    if info.is_dir():
                        continue

                    logical_path = _member_path(prefix, info.filename)
                    if _is_raw_archive(info.filename):
                        nested_zip = _copy_member_to_temp(zip_ref, info.filename)
                        pending.append(
                            (
                                nested_zip,
                                _member_path(prefix, _nested_prefix(info.filename)),
                                depth + 1,
                                True,
                            )
                        )
                        continue

                    members.append(
                        ArchiveMember(
                            relative_path=logical_path,
                            file_size_bytes=info.file_size,
                            compressed_size_bytes=info.compress_size,
                            crc32=info.CRC,
                            zip_depth=depth,
                        )
                    )
        finally:
            if delete_after:
                current_zip.unlink(missing_ok=True)

    members.sort(key=lambda item: item.relative_path)
    return members


def _extract_selected_from_zip(
    zip_path: Path,
    extract_dir: Path,
    selected: set[str],
    *,
    prefix: str,
    depth: int,
    max_nesting: int,
) -> int:
    if depth >= max_nesting:
        raise RuntimeError("hit max zip nesting limit")

    extracted = 0
    with ZipFile(zip_path, "r") as zip_ref:
        for info in zip_ref.infolist():
            if info.is_dir():
                continue

            logical_path = _member_path(prefix, info.filename)
            if _is_raw_archive(info.filename):
                nested_zip = _copy_member_to_temp(zip_ref, info.filename)
                try:
                    extracted += _extract_selected_from_zip(
                        nested_zip,
                        extract_dir,
                        selected,
                        prefix=_member_path(prefix, _nested_prefix(info.filename)),
                        depth=depth + 1,
                        max_nesting=max_nesting,
                    )
                finally:
                    nested_zip.unlink(missing_ok=True)
                continue

            if logical_path not in selected:
                continue

            target = extract_dir / logical_path
            target.parent.mkdir(parents=True, exist_ok=True)
            with zip_ref.open(info, "r") as source, target.open("wb") as dest:
                shutil.copyfileobj(source, dest)
            extracted += 1

    return extracted


def extract_archive_members(
    zip_path: Path,
    extract_dir: Path,
    relative_paths: Iterable[str],
    *,
    max_nesting: int = 3,
) -> int:
    """Extract selected logical member paths from a transport zip."""
    selected = {normalize_member_path(path) for path in relative_paths}
    if not selected:
        return 0
    extract_dir.mkdir(parents=True, exist_ok=True)
    return _extract_selected_from_zip(
        zip_path,
        extract_dir,
        selected,
        prefix="",
        depth=0,
        max_nesting=max_nesting,
    )
