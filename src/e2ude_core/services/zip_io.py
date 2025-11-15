from zipfile import ZipFile
import tempfile
import shutil
from typing import List
from enum import StrEnum
from pathlib import Path

import logging
from typing import Tuple, Union


class UnzipContext:
    """
    Context manager to recursively unzip an archive to a temporary
    directory, identify all contained files, and auto-cleanup on exit.
    """

    def __init__(self, zip_path: Union[str, Path]):
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(f"Zip file not found: {self.zip_path}")

        self.temp_dir: str = None
        self.file_list: List[Tuple[Path, Union[FileType, str]]] = []

    def __enter__(self):
        """Create temp dir, unzip, and build file list."""
        self.temp_dir = Path(tempfile.mkdtemp())
        temp_dir_path = self.temp_dir
        logging.info(f"Extracting '{self.zip_path.name}' to {self.temp_dir}")

        # 1. Unzip all files recursively
        recursive_unzip(self.temp_dir, self.zip_path)

        # 2. Walk all files and categorize them
        for file_path in temp_dir_path.rglob("*"):
            if not file_path.is_file():
                continue

            # Get path relative to the /temp_dir
            relative_path = file_path.relative_to(temp_dir_path)

            found_type = "UNKNOWN"

            # 3. Check against patterns
            # We use relative_path.match() which works with glob patterns
            # like '**/file' and 'file'
            for file_type, pattern in file_type_patterns.items():
                if relative_path.match(pattern):
                    found_type = file_type  # Store the Enum object
                    break  # Stop at first match

            self.file_list.append((found_type, file_path))

        # Return the 'self' object to be used after 'as'
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Clean up the temporary directory."""
        if self.temp_dir and Path(self.temp_dir).exists():
            logging.info(f"Cleaning up temp dir: {self.temp_dir}")
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                logging.error(f"Failed to delete temp dir {self.temp_dir}: {e}")

        # Allow exceptions to propagate.


class FileType(StrEnum):
    UNKNOWN = "UNKNOWN"
    # --- Existing Types ---
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
    # 'MAINT_DAT' omitted (legacy/unused)
    MAINT_XML = "MAINT_XML"
    MAINT_EVT = "MAINT_EVT"
    MAINT_PRM = "MAINT_PRM"
    TMPTR_LOG = "TMPTR_LOG"
    MAINT_LOG = "MAINT_LOG"

    # Root-level CSV files
    # TODO: Extract metadata CSV categories into explicit enums/handlers.
    METADATA_CSV = "METADATA_CSV"

    # Files from _RSM_RawArchive/RSM/*_MAINT_*
    CSFIR_DAT = "CSFIR_DAT"
    LENG_EFF_DAT = "LENG_EFF_DAT"
    RENG_EFF_DAT = "RENG_EFF_DAT"
    LENG_PERF = "LENG_PERF"
    RENG_PERF = "RENG_PERF"
    SDRS_DAT = "SDRS_DAT"

    # Files from _RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT
    ERR_1553 = "ERR_1553"
    COMM_BIT = "COMM_BIT"
    INCDS_BIT = "INCDS_BIT"
    LENG_BIT = "LENG_BIT"
    RENG_BIT = "RENG_BIT"
    VEHCL_BIT = "VEHCL_BIT"

    # Files from _RSM_RawArchive/DIA_MAINTENANCE
    DIA_MAINT_SUMMARY = "DIA_MAINT_SUMMARY"
    DIA_MAINT_DETAIL = "DIA_MAINT_DETAIL"
    DIA_MAINT_STATUS = "DIA_MAINT_STATUS"


file_type_patterns = {
    FileType.MCDATA: "*_MCData",
    FileType.SEGMENTS: "*_Segments",
    FileType.VERSIONS: "*_Versions.xml",
    FileType.GSEVENTS: "*_GSEvents.xml",
    FileType.FLIGHTSYSTEMS: "*_FlightSystems",
    FileType.AR: "*_AR.txt",
    FileType.STATUS: "*_Status.txt",
    FileType.ENGINE: "*_Engine",
    FileType.AIRCRAFT_CONFIG: "*_AircraftConfiguration.xml",
    FileType.ACAWS_LOG: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_ACAWS_LOG",
    FileType.MAINT_XML: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.xml",
    FileType.MAINT_EVT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.evt",
    FileType.MAINT_PRM: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.prm",
    FileType.TMPTR_LOG: "*_RSM_RawArchive/RSM/TMPTR_LOG",
    FileType.MAINT_LOG: "*_RSM_RawArchive/RSM/MAINT_LOG",
    # Catches all .csv files at the root
    FileType.METADATA_CSV: "*.csv",
    # Files from _RSM_RawArchive/RSM/*_MAINT_*
    FileType.CSFIR_DAT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_CSFIR/*_CSFIR_DAT",
    FileType.LENG_EFF_DAT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_EFF/*_LENG_EFF_DAT",
    FileType.RENG_EFF_DAT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_EFF/*_RENG_EFF_DAT",
    FileType.LENG_PERF: "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_PERF/*_LENG_PERF",
    FileType.RENG_PERF: "*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_PERF/*_RENG_PERF",
    FileType.SDRS_DAT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_SDRS/*_SDRS_DAT",
    # Files from _RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT
    FileType.ERR_1553: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_1553_ERR",
    FileType.COMM_BIT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_COMM_BIT",
    FileType.INCDS_BIT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_INCDS_BIT",
    FileType.LENG_BIT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_LENG_BIT",
    FileType.RENG_BIT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_RENG_BIT",
    FileType.VEHCL_BIT: "*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_VEHCL_BIT",
    # Files from _RSM_RawArchive/DIA_MAINTENANCE
    FileType.DIA_MAINT_SUMMARY: "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/maint_summary_data.txt",
    FileType.DIA_MAINT_DETAIL: "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/*_detailed_data.txt",
    FileType.DIA_MAINT_STATUS: "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/system_snapshot_fault_status.txt",
}


def recursive_unzip(extract_dir, zip_path):
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    to_unzip = [Path(new_zip_path) for new_zip_path in Path(extract_dir).glob("*.zip")]

    MAX_NESTING = 3
    nesting = 1
    while to_unzip and nesting < MAX_NESTING:
        zip_path = to_unzip.pop()
        extract_dir = Path(zip_path.parent / zip_path.stem)
        extract_dir.mkdir()
        with ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        zip_path.unlink()
        for new_zip_path in extract_dir.glob("*.zip"):
            to_unzip.append(Path(new_zip_path))
        nesting += 1

        if nesting == MAX_NESTING:
            raise Exception("ERROR: hit max nesting limit")
