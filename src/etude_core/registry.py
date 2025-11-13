from typing import Dict
from etude_core.pipelines.base import FileHandler

from etude_core.db.models import TmptrData

from etude_core.pipelines.parsers.tmptr import parse_tmptr_dataframe
from etude_core.services.zip_io import FileType

HANDLER_REGISTRY: Dict[str, FileHandler] = {
    # 1. SIMPLE CASE
    FileType.TMPTR_LOG.value: FileHandler(
        pipeline_id="tmptr_log",
        parser_func=parse_tmptr_dataframe,
        table_config=TmptrData,
    )
}
