from io import BytesIO
from zipfile import ZipFile

from e2ude_core.services.zip_io import UnzipContext


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


def test_unzip_context_extracts_nested_archives(tmp_path):
    zip_path = tmp_path / "sample_TransportRSM.fpkg.e2d.zip"
    _build_sample_zip(zip_path)

    with UnzipContext(zip_path) as ctx:
        extracted = (
            ctx.temp_dir
            / "123456_20240101_000000_000_RSM_RawArchive"
            / "RSM"
            / "TMPTR_LOG"
        )
        assert extracted.exists()
        assert extracted.read_text(encoding="utf-8") == "tmptr payload"

        segments = ctx.temp_dir / "123456_20240101_000000_000_Segments"
        assert segments.exists()


def test_unzip_context_cleans_up_temp_dir(tmp_path):
    zip_path = tmp_path / "sample_TransportRSM.fpkg.e2d.zip"
    _build_sample_zip(zip_path)

    with UnzipContext(zip_path) as ctx:
        temp_dir = ctx.temp_dir
        assert temp_dir.exists()

    assert not temp_dir.exists()
