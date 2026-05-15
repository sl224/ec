"""Runtime file specs used for zip-member matching and parser dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from pathlib import PurePosixPath
from typing import Callable, Iterable, Sequence, Type

import pandas as pd

from e2ude_core.db.models import (
    Base,
    LcsTemp,
    McInDiscr,
    NavData,
    PfcDb,
    RadarState,
    RfcDb,
    RotoScan,
    Rpcs,
    RpcsPres,
    SegmentsData,
    TmptrData,
    EngineOnOff,
)
from e2ude_core.pipelines.parsers import (
    parse_engine_on_off_dataframe,
    parse_mcdata,
    parse_segment,
    parse_tmptr_dataframe,
)


class FileType(StrEnum):
    UNKNOWN = "UNKNOWN"
    MCDATA = "MCDATA"
    SEGMENTS = "SEGMENTS"
    VERSIONS = "VERSIONS"
    STATUS = "STATUS"
    GSEVENTS = "GS_EVENTS"
    FLIGHTSYSTEMS = "FLIGHT_SYSTEMS"
    ENGINE = "ENGINE"
    ENGINE_ON_OFF = "ENGINE_ON_OFF"
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


ParserFunc = Callable[[Path], dict[Type[Base], pd.DataFrame]]


@dataclass(frozen=True)
class RuntimeFileSpec:
    file_type: FileType
    match_patterns: tuple[str, ...]
    parser_id: str | None = None
    version: int | None = None
    parser_func: ParserFunc | None = None
    expected_models: tuple[Type[Base], ...] = ()

    @property
    def is_handled(self) -> bool:
        return (
            self.parser_id is not None
            and self.version is not None
            and self.parser_func is not None
            and bool(self.expected_models)
        )


@dataclass(frozen=True)
class StageDependencySpec:
    name: str
    match_patterns: tuple[str, ...]


def coerce_file_type(value: FileType | str | None) -> FileType:
    if isinstance(value, FileType):
        return value
    if value is None:
        return FileType.UNKNOWN
    try:
        return FileType(value)
    except ValueError:
        return FileType.UNKNOWN


RUNTIME_FILE_SPECS: tuple[RuntimeFileSpec, ...] = (
    RuntimeFileSpec(
        FileType.MCDATA,
        ("*_MCData",),
        "mcdata",
        1,
        parse_mcdata,
        (
            NavData,
            Rpcs,
            RpcsPres,
            RadarState,
            RotoScan,
            PfcDb,
            RfcDb,
            LcsTemp,
            McInDiscr,
        ),
    ),
    RuntimeFileSpec(
        FileType.SEGMENTS,
        ("*_Segments",),
        "segments",
        1,
        parse_segment,
        (SegmentsData,),
    ),
    RuntimeFileSpec(FileType.VERSIONS, ("*_Versions.xml",)),
    RuntimeFileSpec(FileType.GSEVENTS, ("*_GSEvents.xml",)),
    RuntimeFileSpec(FileType.FLIGHTSYSTEMS, ("*_FlightSystems",)),
    RuntimeFileSpec(FileType.AR, ("*_AR.txt",)),
    RuntimeFileSpec(FileType.STATUS, ("*_Status.txt",)),
    RuntimeFileSpec(
        FileType.ENGINE_ON_OFF,
        ("*_Engine",),
        "engine_on_off",
        2,
        parse_engine_on_off_dataframe,
        (EngineOnOff,),
    ),
    RuntimeFileSpec(FileType.AIRCRAFT_CONFIG, ("*_AircraftConfiguration.xml",)),
    RuntimeFileSpec(
        FileType.ACAWS_LOG,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_ACAWS_LOG",),
    ),
    RuntimeFileSpec(
        FileType.MAINT_XML,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.xml",),
    ),
    RuntimeFileSpec(
        FileType.MAINT_EVT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.evt",),
    ),
    RuntimeFileSpec(
        FileType.MAINT_PRM,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT.prm",),
    ),
    RuntimeFileSpec(
        FileType.TMPTR_LOG,
        ("*_RSM_RawArchive/RSM/TMPTR_LOG",),
        "tmptr_log",
        2,
        parse_tmptr_dataframe,
        (TmptrData,),
    ),
    RuntimeFileSpec(FileType.MAINT_LOG, ("*_RSM_RawArchive/RSM/MAINT_LOG",)),
    RuntimeFileSpec(FileType.METADATA_CSV, ("*.csv",)),
    RuntimeFileSpec(
        FileType.CSFIR_DAT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_CSFIR/*_CSFIR_DAT",),
    ),
    RuntimeFileSpec(
        FileType.LENG_EFF_DAT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_EFF/*_LENG_EFF_DAT",),
    ),
    RuntimeFileSpec(
        FileType.RENG_EFF_DAT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_EFF/*_RENG_EFF_DAT",),
    ),
    RuntimeFileSpec(
        FileType.LENG_PERF,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_PERF/*_LENG_PERF",),
    ),
    RuntimeFileSpec(
        FileType.RENG_PERF,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_ENG_PERF/*_RENG_PERF",),
    ),
    RuntimeFileSpec(
        FileType.SDRS_DAT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_SDRS/*_SDRS_DAT",),
    ),
    RuntimeFileSpec(
        FileType.ERR_1553,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_1553_ERR",),
    ),
    RuntimeFileSpec(
        FileType.COMM_BIT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_COMM_BIT",),
    ),
    RuntimeFileSpec(
        FileType.INCDS_BIT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_INCDS_BIT",),
    ),
    RuntimeFileSpec(
        FileType.LENG_BIT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_LENG_BIT",),
    ),
    RuntimeFileSpec(
        FileType.RENG_BIT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_RENG_BIT",),
    ),
    RuntimeFileSpec(
        FileType.VEHCL_BIT,
        ("*_RSM_RawArchive/RSM/*_MAINT_*/*_MAINT_DAT/*_VEHCL_BIT",),
    ),
    RuntimeFileSpec(
        FileType.DIA_MAINT_SUMMARY,
        ("*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/maint_summary_data.txt",),
    ),
    RuntimeFileSpec(
        FileType.DIA_MAINT_DETAIL,
        ("*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/*_detailed_data.txt",),
    ),
    RuntimeFileSpec(
        FileType.DIA_MAINT_STATUS,
        (
            "*_RSM_RawArchive/DIA_MAINTENANCE/*_maintenance_data/system_snapshot_fault_status.txt",
        ),
    ),
)


STAGE_DEPENDENCIES: tuple[StageDependencySpec, ...] = (
    StageDependencySpec("nested_raw_archive", ("*RSM_RawArchive.zip",)),
)


def _pattern_sort_key(pattern: str) -> tuple[int, int, int, int, int, str]:
    posix_path = PurePosixPath(pattern)
    parts = posix_path.parts
    wildcard_chars = sum(1 for char in pattern if char in "*?[")
    literal_chars = len(pattern) - wildcard_chars
    literal_parts = sum(1 for part in parts if not any(char in part for char in "*?["))
    return (
        -literal_parts,
        -len(parts),
        -literal_chars,
        wildcard_chars,
        -len(pattern),
        pattern,
    )


def _build_catalog_patterns(
    specs: Iterable[RuntimeFileSpec] | None = None,
) -> tuple[tuple[FileType, str], ...]:
    specs = RUNTIME_FILE_SPECS if specs is None else specs
    patterns = [
        (spec.file_type, pattern) for spec in specs for pattern in spec.match_patterns
    ]
    return tuple(
        sorted(
            patterns,
            key=lambda item: (_pattern_sort_key(item[1]), item[0].value),
        )
    )


CATALOG_FILE_PATTERNS: tuple[tuple[FileType, str], ...] = _build_catalog_patterns()


CURRENT_ARCHIVE_CATALOG_VERSION = 1
CURRENT_METADATA_CATALOG_GENERATION = CURRENT_ARCHIVE_CATALOG_VERSION


HANDLED_FILE_SPECS: tuple[RuntimeFileSpec, ...] = tuple(
    spec for spec in RUNTIME_FILE_SPECS if spec.is_handled
)
HANDLED_FILE_SPECS_BY_TYPE: dict[FileType, RuntimeFileSpec] = {
    spec.file_type: spec for spec in HANDLED_FILE_SPECS
}


def parser_id_for(spec: RuntimeFileSpec) -> str:
    if spec.parser_id is None:
        raise ValueError("handled parser spec must have a parser_id")
    return spec.parser_id


def artifact_key_for(spec: RuntimeFileSpec, model: Type[Base]) -> str:
    parser_id = parser_id_for(spec)
    if len(spec.expected_models) == 1:
        return parser_id
    return f"{parser_id}.{model.__name__}"


def normalize_member_path(relative_path: Path | str) -> str:
    return str(PurePosixPath(str(relative_path).replace("\\", "/")))


def path_matches_pattern(relative_path: Path | str, pattern: str) -> bool:
    return PurePosixPath(normalize_member_path(relative_path)).match(pattern)


def spec_matches_path(spec: RuntimeFileSpec, relative_path: Path | str) -> bool:
    return any(
        path_matches_pattern(relative_path, pattern) for pattern in spec.match_patterns
    )


def spec_for_path(relative_path: Path | str) -> RuntimeFileSpec | None:
    for spec in RUNTIME_FILE_SPECS:
        if spec_matches_path(spec, relative_path):
            return spec
    return None


def handled_specs_for_path(relative_path: Path | str) -> tuple[RuntimeFileSpec, ...]:
    return tuple(
        spec for spec in HANDLED_FILE_SPECS if spec_matches_path(spec, relative_path)
    )


def detect_file_type(relative_path: Path | str) -> FileType:
    spec = spec_for_path(relative_path)
    return FileType.UNKNOWN if spec is None else spec.file_type


def build_active_stage_patterns(active_types: Sequence[FileType]) -> list[str]:
    active_set = set(active_types)
    patterns: list[str] = []

    for spec in RUNTIME_FILE_SPECS:
        if spec.file_type in active_set:
            patterns.extend(spec.match_patterns)

    for dependency in STAGE_DEPENDENCIES:
        patterns.extend(dependency.match_patterns)

    return patterns
