from io import BytesIO
from zipfile import ZipFile

from e2ude_core.services.zip_io import extract_archive_members


def _build_sample_zip(zip_path):
    nested_buffer = BytesIO()
    with ZipFile(nested_buffer, "w") as nested_zip:
        nested_zip.writestr("RSM/TMPTR_LOG", "tmptr payload")

    with ZipFile(zip_path, "w") as root_zip:
        root_zip.writestr("123456_20240101_000000_000_MCData", "mcdata payload")
        root_zip.writestr(
            "123456_20240101_000000_000_Segments",
            "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,,,,,,,PreFlight,\n",
        )
        root_zip.writestr(
            "123456_20240101_000000_000_RSM_RawArchive.zip",
            nested_buffer.getvalue(),
        )


def test_extract_archive_members_extracts_selected_nested_members(tmp_path):
    zip_path = tmp_path / "sample_TransportRSM.fpkg.e2d.zip"
    _build_sample_zip(zip_path)

    extract_dir = tmp_path / "extract"
    count = extract_archive_members(
        zip_path,
        extract_dir,
        [
            "123456_20240101_000000_000_RSM_RawArchive/RSM/TMPTR_LOG",
            "123456_20240101_000000_000_Segments",
        ],
    )

    assert count == 2
    extracted = (
        extract_dir / "123456_20240101_000000_000_RSM_RawArchive" / "RSM" / "TMPTR_LOG"
    )
    assert extracted.read_text(encoding="utf-8") == "tmptr payload"
    assert (extract_dir / "123456_20240101_000000_000_Segments").exists()
