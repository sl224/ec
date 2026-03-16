import logging
import shutil
import tempfile
from pathlib import Path
from typing import Union
from zipfile import ZipFile

logger = logging.getLogger(__name__)


class UnzipContext:
    """Extract a transport zip to a temporary directory."""

    def __init__(self, zip_path: Union[str, Path]):
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(f"Zip file not found: {self.zip_path}")

        self.temp_dir: Path | None = None

    def __enter__(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        logger.info("Extracting '%s' to %s", self.zip_path.name, self.temp_dir)

        _recursive_unzip(self.temp_dir, self.zip_path)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.temp_dir and Path(self.temp_dir).exists():
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as exc:
                logger.error("Failed to delete temp dir %s: %s", self.temp_dir, exc)


def _recursive_unzip(extract_dir: Path, zip_path: Path):
    with ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)

    to_unzip = [Path(new_zip_path) for new_zip_path in Path(extract_dir).glob("*.zip")]

    max_nesting = 3
    nesting = 1
    while to_unzip and nesting < max_nesting:
        nested_zip_path = to_unzip.pop()
        nested_extract_dir = nested_zip_path.parent / nested_zip_path.stem
        nested_extract_dir.mkdir(exist_ok=True)

        with ZipFile(nested_zip_path, "r") as zip_ref:
            zip_ref.extractall(nested_extract_dir)

        nested_zip_path.unlink()
        for new_zip_path in nested_extract_dir.glob("*.zip"):
            to_unzip.append(Path(new_zip_path))

        nesting += 1

    if to_unzip:
        raise RuntimeError("ERROR: hit max nesting limit")
