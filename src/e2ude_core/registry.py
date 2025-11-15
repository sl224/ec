from typing import Dict

from e2ude_core.pipelines.base import FileHandler
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


# Maps a file type string to its corresponding FileHandler instance.
HANDLER_REGISTRY: Dict[str, FileHandler] = {
    # A "simple" handler for a file type that maps to a single table.
    FileType.TMPTR_LOG.value: FileHandler(
        pipeline_id="tmptr_log",
        parser_func=parse_tmptr_dataframe,
        table_config=[TmptrData],
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
