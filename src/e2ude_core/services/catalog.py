import hashlib
import logging
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import List

from e2ude_core.services.fs_engine import ParallelFileScanner

logger = logging.getLogger(__name__)


class FileType(StrEnum):
    UNKNOWN = "UNKNOWN"
    MCDATA = "MCDATA"
    SEGMENTS = "SEGMENTS"
    VERSIONS = "VERSIONS"
    STATUS = "STATUS"
    GSEVENTS = "GS_EVENTS"
    FLIGHTSYSTEMS = "FLIGHT_SYSTEMS"
    ENGINE = "ENGINE"
    AR = "AR"
    AIRCRAFT_CONFIG = "AIRCRAFT_CONFIGURATION"
    ACAWS_LOG = "ACAWS_LOG"
    MAINT_XML = "MAINT_XML"
    MAINT_EVT = "MAINT_EVT"
    MAINT_PRM = "MAINT_PRM"
    TMPTR_LOG = "TMPTR_LOG"
    MAINT_LOG = "MAINT_LOG"
    METADATA_CSV = "METADATA_CSV"
    CSFIR_DAT = "CSFIR_DAT"
    LENG_EFF_DAT = "LENG_EFF_DAT"
    RENG_EFF_DAT = "RENG_EFF_DAT"
    LENG_PERF = "LENG_PERF"
    RENG_PERF = "RENG_PERF"
    SDRS_DAT = "SDRS_DAT"
    ERR_1553 = "ERR_1553"
    COMM_BIT = "COMM_BIT"
    INCDS_BIT = "INCDS_BIT"
    LENG_BIT = "LENG_BIT"
    RENG_BIT = "RENG_BIT"
    VEHCL_BIT = "VEHCL_BIT"
    DIA_MAINT_SUMMARY = "DIA_MAINT_SUMMARY"
    DIA_MAINT_DETAIL = "DIA_MAINT_DETAIL"
    DIA_MAINT_STATUS = "DIA_MAINT_STATUS"


# Pattern Registry: List of (Enum, PatternString)
FILE_PATTERNS = [
    (FileType.MCDATA, "*_MCData"),
    (FileType.SEGMENTS, "*_Segments"),
    (FileType.VERSIONS, "*_Versions.xml"),
    (FileType.GSEVENTS, "*_GSEvents.xml"),
    (FileType.FLIGHTSYSTEMS, "*_FlightSystems"),
    (FileType.AR, "*_AR.txt"),
    (FileType.STATUS, "*_Status.txt"),
    (FileType.ENGINE, "*_Engine"),
    (FileType.AIRCRAFT_CONFIG, "*_AircraftConfiguration.xml"),
    (FileType.ACAWS_LOG, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_ACAWS_LOG"),
    (FileType.MAINT_XML, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.xml"),
    (FileType.MAINT_EVT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.evt"),
    (FileType.MAINT_PRM, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.prm"),
    (FileType.TMPTR_LOG, "*_RSM_RawArchive/RSM/TMPTR_LOG"),
    (FileType.MAINT_LOG, "*_RSM_RawArchive/RSM/MAINT_LOG"),
    (FileType.METADATA_CSV, "*.csv"),
    (FileType.CSFIR_DAT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_CSFIR/*_CSFIR_DAT"),
    (FileType.LENG_EFF_DAT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_EFF/*_LENG_EFF_DAT"),
    (FileType.RENG_EFF_DAT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_EFF/*_RENG_EFF_DAT"),
    (FileType.LENG_PERF, "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_PERF/*_LENG_PERF"),
    (FileType.RENG_PERF, "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_PERF/*_RENG_PERF"),
    (FileType.SDRS_DAT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_SDRS/*_SDRS_DAT"),
    (FileType.ERR_1553, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_1553_ERR"),
    (FileType.COMM_BIT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_COMM_BIT"),
    (FileType.INCDS_BIT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_INCDS_BIT"),
    (FileType.LENG_BIT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_LENG_BIT"),
    (FileType.RENG_BIT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_RENG_BIT"),
    (FileType.VEHCL_BIT, "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_VEHCL_BIT"),
    (
        FileType.DIA_MAINT_SUMMARY,
        "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/maint_summary_data.txt",
    ),
    (
        FileType.DIA_MAINT_DETAIL,
        "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/*_detailed_data.txt",
    ),
    (
        FileType.DIA_MAINT_STATUS,
        "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/system_snapshot_fault_status.txt",
    ),
]


@dataclass(slots=True, frozen=True)
class FileScanResult:
    """
    Output struct for cataloging.
    slots=True prevents dynamic dict creation, saving memory per file.
    """

    relative_path: str
    file_type: str
    file_size_bytes: int
    md5: bytes


def _calculate_stream_md5(f_obj, chunk_size=65536) -> bytes:
    hash_md5 = hashlib.md5()
    while chunk := f_obj.read(chunk_size):
        hash_md5.update(chunk)
    return hash_md5.digest()


def _catalog_filter_predicate(entry: os.DirEntry) -> bool:
    """
    Optimization: We could add logic here to skip strictly irrelevant files
    before they hit the worker thread. For now, pass all.
    """
    return True


def _process_staged_file(path: Path, root_dir: Path) -> FileScanResult:
    """
    The Worker Function.
    Calculates hash and determines type for a file already on the SSD.
    """
    relative_path = path.relative_to(root_dir)

    f_type = FileType.UNKNOWN.value
    for f_enum, pattern in FILE_PATTERNS:
        if relative_path.match(pattern):
            f_type = f_enum.value
            break

    try:
        with open(path, "rb") as f:
            md5 = _calculate_stream_md5(f)

        size = path.stat().st_size

        return FileScanResult(
            relative_path=str(relative_path),
            file_type=f_type,
            file_size_bytes=size,
            md5=md5,
        )
    except OSError as e:
        logger.error(f"Failed to hash staged file {path}: {e}")
        return FileScanResult(
            relative_path=str(relative_path),
            file_type=FileType.UNKNOWN.value,
            file_size_bytes=0,
            md5=b"",
        )


def catalog_staged_folder(staged_root: Path) -> List[FileScanResult]:
    """
    Uses the Parallel Engine to catalog a local folder.
    """
    scanner = ParallelFileScanner(max_workers=16)

    # We use a lambda to close over `staged_root` for the action function
    return scanner.walk(
        root_path=staged_root,
        filter_func=_catalog_filter_predicate,
        action_func=lambda p: _process_staged_file(p, staged_root),
    )
