# %%
from pathlib import Path
import pytest

from etude_core.services.zip_io import UnzipContext, FileType


STATIC_ASSETS_ROOT = Path("tests/static_assets")


def test_zip_manager():
    test_zip = (
        STATIC_ASSETS_ROOT / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
    )
    with UnzipContext(test_zip) as c:
        assert Path(c.temp_dir).exists()
        assert len(c.file_list) > 0

        # Check that we found at least one of a specific file type
        found_acaws = any(ft == FileType.ACAWS_LOG for ft, _ in c.file_list)
        assert found_acaws, "ACAWS_LOG file not found in extracted files."

    # After exiting the context, the temp directory should be gone
    assert not Path(c.temp_dir).exists()


def test_zip_manager_print_files():
    test_zip = (
        STATIC_ASSETS_ROOT / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
    )
    with UnzipContext(test_zip) as c:
        for ft, f in c.file_list:
            print(ft, f)


def test_zip_manager_file_not_found():
    with pytest.raises(FileNotFoundError):
        with UnzipContext("non_existent_file.zip"):
            pass  # This code should not be reached


if __name__ == "__main__":
    test_zip_manager_print_files()
