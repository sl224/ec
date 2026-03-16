"""Handler lookup built from the runtime file specs."""

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Type

import pandas as pd

from e2ude_core.db.models import Base
from e2ude_core.runtime_files import FileType, PipelineId, iter_handled_file_specs


@dataclass(frozen=True)
class HandlerSpec:
    pipeline_id: PipelineId
    version: int
    parser_func: Callable[[Path], dict[Type[Base], pd.DataFrame]]
    expected_models: tuple[Type[Base], ...]


# Built from `runtime_files.py`.
HANDLER_REGISTRY: dict[FileType, HandlerSpec] = {
    spec.file_type: HandlerSpec(
        pipeline_id=spec.pipeline_id,
        version=spec.version,
        parser_func=spec.parser_func,
        expected_models=spec.expected_models,
    )
    for spec in iter_handled_file_specs()
}
