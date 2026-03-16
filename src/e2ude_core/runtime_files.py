"""Runtime file specs used by staging, detection, and handler dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
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
)
from e2ude_core.pipelines.parsers import (
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


@dataclass(frozen=True, order=True)
class PipelineId:
    value: str

    def __post_init__(self):
        normalized = self.value.strip()
        if not normalized:
            raise ValueError("pipeline_id must be a non-empty string")
        object.__setattr__(self, "value", normalized)

    def __str__(self) -> str:
        return self.value


ParserFunc = Callable[[Path], dict[Type[Base], pd.DataFrame]]


@dataclass(frozen=True)
class RuntimeFileSpec:
    file_type: FileType
    match_patterns: tuple[str, ...]
    pipeline_id: PipelineId | None = None
    version: int | None = None
    parser_func: ParserFunc | None = None
    expected_models: tuple[Type[Base], ...] = ()

    @property
    def is_handled(self) -> bool:
        return (
            self.pipeline_id is not None
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
        PipelineId("mcdata"),
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
        PipelineId("segments"),
        1,
        parse_segment,
        (SegmentsData,),
    ),
    RuntimeFileSpec(FileType.VERSIONS, ("*_Versions.xml",)),
    RuntimeFileSpec(FileType.GSEVENTS, ("*_GSEvents.xml",)),
    RuntimeFileSpec(FileType.FLIGHTSYSTEMS, ("*_FlightSystems",)),
    RuntimeFileSpec(FileType.AR, ("*_AR.txt",)),
    RuntimeFileSpec(FileType.STATUS, ("*_Status.txt",)),
    RuntimeFileSpec(FileType.ENGINE, ("*_Engine",)),
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
        PipelineId("tmptr_log"),
        1,
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


RUNTIME_FILE_SPECS_BY_TYPE = {spec.file_type: spec for spec in RUNTIME_FILE_SPECS}

CATALOG_FILE_PATTERNS: tuple[tuple[FileType, str], ...] = tuple(
    (spec.file_type, pattern)
    for spec in RUNTIME_FILE_SPECS
    for pattern in spec.match_patterns
)


def iter_handled_file_specs() -> Iterable[RuntimeFileSpec]:
    return (spec for spec in RUNTIME_FILE_SPECS if spec.is_handled)


def build_active_stage_patterns(active_types: Sequence[FileType]) -> list[str]:
    active_set = set(active_types)
    patterns: list[str] = []

    for spec in RUNTIME_FILE_SPECS:
        if spec.file_type in active_set:
            patterns.extend(spec.match_patterns)

    for dependency in STAGE_DEPENDENCIES:
        patterns.extend(dependency.match_patterns)

    return patterns
