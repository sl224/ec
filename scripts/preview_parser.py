from __future__ import annotations

import argparse
from pathlib import Path

from e2ude_core.cli import main as cli_main


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one source file through a registered parser and print JSON."
    )
    parser.add_argument("file_path", type=Path)
    parser.add_argument(
        "--file-type",
        help="Parser id or file type to use when the path does not identify it.",
    )
    parser.add_argument("--head", type=int, default=5)
    args = parser.parse_args()

    cli_args = ["preview", str(args.file_path), "--head", str(args.head)]
    if args.file_type:
        cli_args.extend(["--as", args.file_type])
    return cli_main(cli_args)


if __name__ == "__main__":
    raise SystemExit(main())
