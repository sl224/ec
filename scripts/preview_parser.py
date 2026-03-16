import argparse
import json
from pathlib import Path

import pandas as pd

from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.services.file_catalog import FileType, detect_file_type


def _resolve_file_type(file_path: Path, explicit_file_type: str | None) -> tuple[str, str]:
    if explicit_file_type:
        return explicit_file_type, "explicit"

    candidates = [Path(*file_path.parts[index:]) for index in range(len(file_path.parts))]
    for candidate in candidates:
        detected = detect_file_type(candidate)
        if detected != FileType.UNKNOWN and detected.value in HANDLER_REGISTRY:
            return detected.value, candidate.as_posix()

    raise ValueError(
        "Could not infer a supported file type from the path. "
        "Pass --file-type to force a registered handler."
    )


def _preview_records(df: pd.DataFrame, head_rows: int) -> list[dict[str, object]]:
    preview_df = df.head(head_rows).astype(object).where(pd.notna(df.head(head_rows)), None)
    return preview_df.to_dict(orient="records")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run a single real file through one registered handler and print a JSON preview. "
            "This is a DB-free local development helper."
        )
    )
    parser.add_argument("file_path", type=Path, help="Path to a staged/extracted source file")
    parser.add_argument(
        "--file-type",
        choices=sorted(HANDLER_REGISTRY.keys()),
        help="Override handler selection when the filename/path does not match the normal pattern",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=5,
        help="Number of preview rows per output table",
    )
    args = parser.parse_args()

    file_path = args.file_path.expanduser().resolve()
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    if args.head <= 0:
        raise ValueError(f"--head must be positive, got {args.head}")

    file_type, selection_source = _resolve_file_type(file_path, args.file_type)
    handler_spec = HANDLER_REGISTRY[file_type]
    payload = handler_spec.parser_func(file_path)

    output_tables = []
    for model in handler_spec.expected_models:
        df = payload.get(model)
        if df is None:
            output_tables.append(
                {
                    "model": model.__name__,
                    "table": model.__tablename__,
                    "rows": None,
                    "columns": [],
                    "preview": [],
                }
            )
            continue

        output_tables.append(
            {
                "model": model.__name__,
                "table": model.__tablename__,
                "rows": int(len(df)),
                "columns": list(df.columns),
                "preview": _preview_records(df, args.head),
            }
        )

    print(
        json.dumps(
            {
                "file_path": str(file_path),
                "selected_file_type": file_type,
                "selection_source": selection_source,
                "pipeline_id": handler_spec.pipeline_id,
                "handler_version": handler_spec.version,
                "tables": output_tables,
            },
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
