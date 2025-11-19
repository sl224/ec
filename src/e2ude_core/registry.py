from typing import Dict

from e2ude_core.pipelines.base import BaseHandler, FileHandler  # Import Base
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
)
from e2ude_core.services.zip_io import FileType
from e2ude_core.pipelines.parsers.tmptr import parse_tmptr_dataframe
from e2ude_core.pipelines.parsers.mc_data import parse_mcdata

# Registry now stores BaseHandler (polymorphic)
HANDLER_REGISTRY: Dict[str, BaseHandler] = {
    FileType.TMPTR_LOG.value: FileHandler(
        pipeline_id="tmptr_log",
        parser_func=parse_tmptr_dataframe,
        table_config=[TmptrData],
    ),
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
