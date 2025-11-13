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

import utils.sql_io as sql_io
import re

import pandas as pd

import sqlalchemy as sa 
import numpy as np
import re

# TODO setup a network drive location for test assets
STATIC_ASSETS_ROOT = Path("tests/static_assets")

import warnings
warnings.filterwarnings("ignore")

import ETL.acaws as acaws

class ACAWS(unittest.TestCase):
    def test_read(self):
        test_path = Path(r'C:\Users\J68531\workspace\E2D_ETL\tests\static_assets\zips\169069_20250203_004745_025_TransportRSM.fpkg.e2d\169069_20250203_004745_025_RSM_RawArchive\RSM\1690690203250047_MAINT_00\1690690203250047_MAINT_DAT\1690690203250047_ACAWS_LOG')
        with open(test_path) as rf:
            lines = rf.readlines()
        df = acaws.get_acaws_df(lines)
        print(df)

    def test_worker(self):
        id_path = (289, r"\\rsiny1-ilsfs\RSM\167929\2023\08\167929_20230808_170206_025_TransportRSM.fpkg.e2d.zip")
        acaws.worker(id_path))



def main_test_suite():
    suite = unittest.TestSuite()
    # suite.addTest(ACAWS("test_read"))
    suite.addTest(ACAWS("test_worker"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    suite = main_test_suite()
    runner.run(suite)

# %%
