from zipfile import ZipFile
import tempfile
import shutil
from enum import StrEnum
from pathlib import Path
import io
import hashlib
import logging
from typing import Union, List, Dict, Any

logger = logging.getLogger(__name__)

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


class UnzipContext:
    """
    Legacy Context manager (kept for compatibility or full extractions).
    """

    def __init__(self, zip_path: Union[str, Path]):
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(f"Zip file not found: {self.zip_path}")

        self.temp_dir: str = None

    def __enter__(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        logging.info(f"Extracting '{self.zip_path.name}' to {self.temp_dir}")

        recursive_unzip(self.temp_dir, self.zip_path)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.temp_dir and Path(self.temp_dir).exists():
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                logging.error(f"Failed to delete temp dir {self.temp_dir}: {e}")


def calculate_stream_md5(f_obj, chunk_size=4096) -> bytes:
    """Calculates MD5 from a file-like object stream."""
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: f_obj.read(chunk_size), b""):
        hash_md5.update(chunk)
    return hash_md5.digest()


class RecursiveZipScanner:
    """
    Scans a zip file recursively in memory (extracting nested zips to temp only when needed),
    calculating hashes and identifying file types without full extraction.
    """
    def __init__(self, root_zip_path: Path):
        self.root_zip_path = root_zip_path
        self.files_found: List[Dict[str, Any]] = []

    def scan(self) -> List[Dict[str, Any]]:
        with ZipFile(self.root_zip_path, 'r') as zf:
            self._scan_zip(zf, Path(""))
        return self.files_found

    def _scan_zip(self, zf: ZipFile, relative_parent: Path):
        for name in zf.namelist():
            # Skip directories
            if name.endswith('/'):
                continue
                
            current_rel_path = relative_parent / name
            
            # Check if it's a nested zip
            if name.lower().endswith('.zip'):
                # For nested zips, we must extract to a temp file to open with zipfile module
                with tempfile.NamedTemporaryFile() as tmp_zip:
                    tmp_zip.write(zf.read(name))
                    tmp_zip.flush()
                    try:
                        with ZipFile(tmp_zip.name, 'r') as nested_zf:
                            self._scan_zip(nested_zf, current_rel_path.with_suffix(''))
                    except Exception as e:
                        logger.warning(f"Failed to scan nested zip {current_rel_path}: {e}")
                continue

            # Identify File Type
            f_type = FileType.UNKNOWN
            for ftype, pattern in file_type_patterns.items():
                if current_rel_path.match(pattern):
                    f_type = ftype
                    break
            
            # Calculate Hash
            with zf.open(name) as f_stream:
                md5 = calculate_stream_md5(f_stream)
                size = zf.getinfo(name).file_size

            self.files_found.append({
                "relative_path": str(current_rel_path),
                "file_type": f_type.value,
                "file_size_bytes": size,
                "md5": md5
            })


def recursive_unzip(extract_dir, zip_path):
    """
    Recursively unzips nested archives.
    """
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    to_unzip = [Path(new_zip_path) for new_zip_path in Path(extract_dir).glob("*.zip")]

    MAX_NESTING = 3
    nesting = 1
    while to_unzip and nesting < MAX_NESTING:
        zip_path = to_unzip.pop()
        extract_dir = Path(zip_path.parent / zip_path.stem)
        extract_dir.mkdir(exist_ok=True)
        with ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        zip_path.unlink()
        for new_zip_path in extract_dir.glob("*.zip"):
            to_unzip.append(Path(new_zip_path))
        nesting += 1

        if nesting == MAX_NESTING:
            raise Exception("ERROR: hit max nesting limit")
