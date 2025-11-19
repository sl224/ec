from typing import Dict

from e2ude_core.pipelines.base import BaseHandler, FileHandler
from e2ude_core.db.models import (
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

# Registry now stores BaseHandler (polymorphic)
HANDLER_REGISTRY: Dict[str, BaseHandler] = {
    FileType.TMPTR_LOG.value: FileHandler(
        pipeline_id="tmptr_log",
        parser_func=parse_tmptr_dataframe,
        table_config=[TmptrData],
    ),
    FileType.SEGMENTS.value: FileHandler(
        pipeline_id="segments",
        parser_func=parse_segment,
        table_config=[SegmentsData],
    ),
    # A "complex" handler for a file type that maps to multiple tables.
    FileType.MCDATA.value: FileHandler(
        pipeline_id="mcdata",
        parser_func=parse_mcdata,
        table_config=[
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
