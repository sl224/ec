from typing import Dict

# --- REFACTOR: No DatasetKey enums needed ---
from etude_core.pipelines.base import FileHandler
from etude_core.db.models import (
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
from etude_core.services.zip_io import FileType

# --- Parsers ---
from etude_core.pipelines.parsers.tmptr import parse_tmptr_dataframe
from etude_core.pipelines.parsers.mc_data import parse_mcdata

# --- Registry ---

HANDLER_REGISTRY: Dict[str, FileHandler] = {
    # --- CASE 1: The "Simple" Parser ---
    FileType.TMPTR_LOG.value: FileHandler(
        pipeline_id="tmptr_log",
        parser_func=parse_tmptr_dataframe,
        table_config=[TmptrData],
    ),
    # --- CASE 2: The "Complex" Parser ---
    FileType.MCDATA.value: FileHandler(
        pipeline_id="mcdata",
        parser_func=parse_mcdata,
        # --- REFACTOR: Just provide a list of expected models ---
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
