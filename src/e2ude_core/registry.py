"""Handler lookup built from the runtime file specs."""

from hashlib import sha1

from e2ude_core.runtime_files import FileType, RuntimeFileSpec, iter_handled_file_specs


# Built from `runtime_files.py`.
HANDLER_REGISTRY: dict[FileType, RuntimeFileSpec] = {
    spec.file_type: spec for spec in iter_handled_file_specs()
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
