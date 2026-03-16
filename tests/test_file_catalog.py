from pathlib import Path
from io import BytesIO
from zipfile import ZipFile

from e2ude_core.services.file_catalog import FileType, catalog_staged_folder
from e2ude_core.services.zip_io import UnzipContext


def _build_sample_zip(zip_path):
    nested_buffer = BytesIO()
    with ZipFile(nested_buffer, "w") as nested_zip:
        nested_zip.writestr("RSM/TMPTR_LOG", "tmptr payload")

    with ZipFile(zip_path, "w") as root_zip:
        root_zip.writestr("123456_20240101_000000_000_MCData", "mcdata payload")
        root_zip.writestr(
            "123456_20240101_000000_000_RSM_RawArchive.zip",
            nested_buffer.getvalue(),
        )


def test_catalog_staged_folder_classifies_unzipped_fixture_structure(tmp_path):
    zip_path = tmp_path / "sample_TransportRSM.fpkg.e2d.zip"
    _build_sample_zip(zip_path)

    with UnzipContext(zip_path) as ctx:
        files = {
            Path(entry.relative_path).as_posix(): entry
            for entry in catalog_staged_folder(ctx.temp_dir)
        }

    assert files["123456_20240101_000000_000_MCData"].file_type == FileType.MCDATA
    assert (
        files["123456_20240101_000000_000_RSM_RawArchive/RSM/TMPTR_LOG"].file_type
        == FileType.TMPTR_LOG
    )
    assert files["123456_20240101_000000_000_MCData"].md5
    assert files["123456_20240101_000000_000_RSM_RawArchive/RSM/TMPTR_LOG"].md5
