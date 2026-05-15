from io import BytesIO
from zipfile import ZipFile

from e2ude_core.runtime_files import FileType, detect_file_type
from e2ude_core.services.zip_io import iter_archive_members


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


def test_archive_member_catalog_classifies_fixture_structure(tmp_path):
    zip_path = tmp_path / "sample_TransportRSM.fpkg.e2d.zip"
    _build_sample_zip(zip_path)

    files = {entry.relative_path: entry for entry in iter_archive_members(zip_path)}

    assert (
        detect_file_type("123456_20240101_000000_000_MCData")
        == FileType.MCDATA
    )
    assert (
        detect_file_type(
            "123456_20240101_000000_000_RSM_RawArchive/RSM/TMPTR_LOG"
        )
        == FileType.TMPTR_LOG
    )
    assert files["123456_20240101_000000_000_MCData"].file_size_bytes
    assert files[
        "123456_20240101_000000_000_RSM_RawArchive/RSM/TMPTR_LOG"
    ].file_size_bytes
