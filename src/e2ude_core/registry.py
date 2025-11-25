from dataclasses import dataclass
from typing import Callable, List, Type
from pathlib import Path
import pandas as pd

from e2ude_core.db.models import (
    Base,
    NavData,
    PfcDb,
    RadarState,
    RfcDb,
    RotoScan,
    Rpcs,
    RpcsPres,
    TmptrData,
    LcsTemp,
    McInDiscr,
    SegmentsData,
)
from e2ude_core.services.zip_io import FileType
from e2ude_core.pipelines.parsers import (
    parse_tmptr_dataframe,
    parse_mcdata,
    parse_segment,
)


@dataclass(frozen=True)
class HandlerSpec:
    pipeline_id: str
    version: int
    parser_func: Callable[[Path], dict[Type[Base], pd.DataFrame]]
    expected_models: List[Type[Base]]


# Registry maps FileType string -> HandlerSpec
HANDLER_REGISTRY: dict[str, HandlerSpec] = {
    FileType.TMPTR_LOG.value: HandlerSpec(
        pipeline_id="tmptr_log",
        version=2,
        parser_func=parse_tmptr_dataframe,
        expected_models=[TmptrData],
    ),
    FileType.SEGMENTS.value: HandlerSpec(
        pipeline_id="segments",
        version=1,
        parser_func=parse_segment,
        expected_models=[SegmentsData],
    ),
    FileType.MCDATA.value: HandlerSpec(
        pipeline_id="mcdata",
        version=3,
        parser_func=parse_mcdata,
        expected_models=[
            NavData,
            Rpcs,
            RpcsPres,
            RadarState,
            RotoScan,
            PfcDb,
            RfcDb,
            LcsTemp,
            McInDiscr,
        ],
    ),
}
