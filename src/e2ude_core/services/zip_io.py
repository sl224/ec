from zipfile import ZipFile
import tempfile
import shutil
from enum import StrEnum
from pathlib import Path
import hashlib
import logging
from typing import Union, List, Dict, Any, Set

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
                            # Mimic recursive_unzip behavior: nested zip contents go into folder named after stem
                            # e.g. 'inner.zip' contents map to 'inner/file.txt'
                            zip_stem = Path(name).stem
                            new_parent = relative_parent / zip_stem
                            self._scan_zip(nested_zf, new_parent)
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
    Legacy full extractor. Recursively unzips nested archives.
    """
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    to_unzip = [Path(new_zip_path) for new_zip_path in Path(extract_dir).glob("*.zip")]

    MAX_NESTING = 3
    nesting = 1
    while to_unzip and nesting < MAX_NESTING:
        zip_path = to_unzip.pop()
        # Logic: extract 'abc.zip' into folder 'abc'
        extract_dir = Path(zip_path.parent / zip_path.stem)
        extract_dir.mkdir(exist_ok=True)
        with ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        zip_path.unlink() # Delete the zip file after extraction
        for new_zip_path in extract_dir.glob("*.zip"):
            to_unzip.append(Path(new_zip_path))
        nesting += 1

        if nesting == MAX_NESTING:
            raise Exception("ERROR: hit max nesting limit")


def extract_specific_files(root_zip_path: Path, target_files: List[str], output_dir: Path):
    """
    Lazily extracts ONLY the requested files from the zip (handling nested zips).
    
    Args:
        root_zip_path: Path to the main zip file.
        target_files: List of relative paths to extract (e.g. ["folder/data.csv", "nested/inner/file.txt"]).
                      These paths MUST match the structure produced by recursive_unzip 
                      (i.e., nested zips are treated as folders).
        output_dir: Physical directory to extract files into.
    """
    targets = set(str(Path(t)) for t in target_files)
    logger.info(f"Lazy extracting {len(targets)} specific files...")
    
    _extract_layer(root_zip_path, Path(""), targets, output_dir)


def _extract_layer(zip_path: Union[Path, str], current_offset: Path, targets: Set[str], output_root: Path):
    """
    Recursive helper for lazy extraction.
    
    Args:
        zip_path: Physical path to the current zip file we are reading.
        current_offset: The virtual path prefix this zip represents (e.g. "subfolder/nested").
        targets: Full set of target paths we are looking for.
        output_root: The base directory where files should eventually land.
    """
    with ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if name.endswith('/'): 
                continue

            # Determine the relative path this member represents in the final structure
            member_path = Path(name)
            
            # If member is "folder/file.txt", full virtual path is "offset/folder/file.txt"
            # If offset is empty, it's just "folder/file.txt"
            full_virtual_path = current_offset / member_path
            full_virtual_str = str(full_virtual_path)

            # Case 1: It's a Nested Zip
            if name.lower().endswith('.zip'):
                # Calculate the virtual folder this zip represents
                # e.g. "data.zip" -> "data"
                zip_virtual_folder = current_offset / member_path.parent / member_path.stem
                zip_virtual_str = str(zip_virtual_folder)

                # Optimization: Do any targets start with this prefix?
                # We check if any target is inside this zip folder.
                # e.g. target "data/config.xml" starts with "data"
                needed = False
                for t in targets:
                    if t.startswith(zip_virtual_str):
                        needed = True
                        break
                
                if needed:
                    # Extract the nested zip to a temp file, process it, then delete it
                    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
                        tmp_zip.write(zf.read(name))
                        tmp_zip_path = tmp_zip.name
                    
                    try:
                        _extract_layer(tmp_zip_path, zip_virtual_folder, targets, output_root)
                    finally:
                        Path(tmp_zip_path).unlink(missing_ok=True)

            # Case 2: It's a regular file
            else:
                # Check for exact match
                if full_virtual_str in targets:
                    # Extract!
                    # We must calculate the physical destination
                    dest_path = output_root / full_virtual_path
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    with dest_path.open('wb') as f_out:
                        f_out.write(zf.read(name))