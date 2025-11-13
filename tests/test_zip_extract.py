# %%
import sys
import os
from pathlib import Path

if Path(os.getcwd()).name != "E2D_ETL":
    ROOT_PATH = os.path.abspath("../")
    assert Path(ROOT_PATH).name == "E2D_ETL"
    if ROOT_PATH not in sys.path:
        sys.path.append(ROOT_PATH)
    os.chdir(ROOT_PATH)

import unittest
import shutil
from rsm_extract import extract

# TODO setup a network drive location for test assets
STATIC_ASSETS_ROOT = Path("tests/static_assets")


class ZipExtract(unittest.TestCase):
    def test_extract_zip_to_user_def_dest(self):
        test_zip = (
            STATIC_ASSETS_ROOT
            / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
        )
        extract_dir = STATIC_ASSETS_ROOT / "temp_dir"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir()
        file_objs = extract(
            test_zip, extract_dir=extract_dir, calc_md5=True, read_lines=True
        )
        for file_obj in file_objs:
            print(file_obj.RelativePath)
            print(file_obj.RawText)
            print("\n\n")

    def test_extract_zip_to_temp(self):
        test_zip = (
            STATIC_ASSETS_ROOT
            / "zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d.zip"
        )
        file_objs = extract(test_zip, calc_md5=True, read_lines=True)
        for file_obj in file_objs:
            print(file_obj.RelativePath)
            print(file_obj.RawText)
            print("\n\n")


def zip_extract_suite():
    suite = unittest.TestSuite()
    suite.addTest(ZipExtract("test_extract_zip_to_temp"))
    suite.addTest(ZipExtract("test_extract_zip_to_user_def_dest"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    suite = zip_extract_suite()
    runner.run(suite)
# %%
