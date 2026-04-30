"""Handler lookup built from the runtime file specs."""

from dataclasses import dataclass
from hashlib import sha1
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


def compute_handler_generation() -> str:
    signature = "\n".join(
        sorted(
            (
                f"{file_type.value}|{handler.pipeline_id.value}|{handler.version}|"
                f"{','.join(model.__tablename__ for model in handler.expected_models)}"
            )
            for file_type, handler in HANDLER_REGISTRY.items()
        )
    )
    return sha1(signature.encode("utf-8")).hexdigest()[:16]


CURRENT_HANDLER_GENERATION = compute_handler_generation()
