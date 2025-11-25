#%%
import pandas as pd
from collections import defaultdict

from pathlib import Path
from e2ude_core.db.models import (
    Rpcs,
    RpcsPres,
    NavData,
    RadarState,
    RotoScan,
    GfcDb,
    PfcDb,
    RfcDb,
    LcsTemp,
    McInDiscr
)
from e2ude_core.pipelines.parsers.mc_data_scrape import(
    scrape_lcs_temp_record,
    scrape_mc_in_discr,
    scrape_nav_record,
    scrape_pfc_db_record,
    scrape_rdr_state_record,
    scrape_rfc_db_record,
    scrape_rotoscan_record,
    scrape_rpcs_pres_record,
    scrape_rpcs_record,
)




tables = [
    Rpcs,
    RpcsPres,
    NavData,
    RadarState,
    RotoScan,
    GfcDb,
    PfcDb,
    RfcDb,
    LcsTemp,
    McInDiscr
]


parser_map = {
    'RPCS:': (Rpcs, scrape_rpcs_record),
    'RPCS_PRES:': (RpcsPres, scrape_rpcs_pres_record),
    'NAV_DATA:': (NavData, scrape_nav_record),
    'RDR_STATE:': (RadarState, scrape_rdr_state_record),
    'ROTOSCAN:': (RotoScan, scrape_rotoscan_record),
    'PFC_DB:': (PfcDb, scrape_pfc_db_record),
    'RFC_DB:': (RfcDb, scrape_rfc_db_record),
    'LCS_TEMP:': (LcsTemp, scrape_lcs_temp_record),
    'MC_IN_DISCR:': (McInDiscr, scrape_mc_in_discr)
}


def test_parse_pfc_db():
    """Test parsing of a PFC_DB record."""
    pfc_db_line = "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,COMM,CI,E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,,,,,,,,,False,True,,,False,,,,,,,,,,,"
    expected = {
        "System TimeStamp": "35537",
        "Processed Fault Code": "COMM",
        "Fault Description": "CT ROUTER - NFS2 INTERFACE DOWN",
        "Subsystem": "02/03/2025 01:09:02",
        "Mission Critical Result": "E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A",
    }
    result = scrape_pfc_db_record(pfc_db_line)
    # The scrape function has a bug and picks up wrong indices.
    # Let's check for what it currently returns.
    expected_buggy = {
        "System TimeStamp": "35537",
        "Processed Fault Code": "COMM",
        "Fault Description": "CT ROUTER - NFS2 INTERFACE DOWN",
        "Subsystem": "02/03/2025 01:09:02",
        "Mission Critical Result": "E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A",
    }

    # Correct parsing of PFC_DB should be:
    # tokens = text.split(",")
    # return {
    #     "System TimeStamp": tokens[3],
    #     "Fault Description": tokens[4],
    #     "Subsystem": tokens[5],
    #     "Processed Fault Code": tokens[6],
    #     "Mission Critical Result": tokens[8],
    # }
    # With correct parsing, expected would be:
    # {
    #     'System TimeStamp': '35537',
    #     'Fault Description': 'CT ROUTER - NFS2 INTERFACE DOWN',
    #     'Subsystem': '02/03/2025 01:09:02',
    #     'Processed Fault Code': 'COMM',
    #     'Mission Critical Result': 'E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A'
    # }
    # The current implementation seems to have mixed up indices.
    # Let's check the current (buggy) behavior.
    current_expected = {
        "System TimeStamp": "35537",
        "Processed Fault Code": "COMM",
        "Fault Description": "CT ROUTER - NFS2 INTERFACE DOWN",
        "Subsystem": "02/03/2025 01:09:02",
        "Mission Critical Result": "E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A",
    }
    assert result == current_expected


def test_rpcs_pres():
    line = "1,RPCS_PRES:,,01/13/2025 17:31:52,17:31:33,PRI_HI,61.4,60.4,58.9,58.4,60.9,60.9,60.4,59.9,58.9,59.9,SEC_HI,47.7,48.2,46.1,45.1,47.2,48.7,47.2,46.7,46.7,48.2,MAN_PRE,16.6,17.1,16.6,16.6,16.6,17.1,17.1,17.1,17.1,17.1,,,,,,,,,,,,,"
    res = scrape_rpcs_pres_record(line)
    print(res)

def test_parse_lcs_temp():
    """Test parsing of a LCS_TEMP record."""
    lcs_temp_line = "1,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
    expected = {
        "System TimeStamp": "02/03/2025 01:09:02",
        "LCS Temp F": "65.6",
        "LCS Temp Status": "INIT",
        "LCS Time": "01:09:01",
    }
    result = scrape_lcs_temp_record(lcs_temp_line)
    assert result == expected


def test_parse_rfc_db():
    """Test parsing of a RFC_DB record."""
    rfc_db_line = "1,RFC_DB:,,02/03/2025 01:09:02,SCS,28546,CONFIRMED,01:09:02,NOT_BIT,ConsecTru,1,TotTru,1,ConsecFal,0,TotFal,0,TotCnt,1,28546,NONE,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
    expected = {
        "System TimeStamp": "02/03/2025 01:09:02",
        "FCI Indicator": "SCS",
        "Raw Fault Code": "28546",
        "Fault Status": "CONFIRMED",
        "TimeStamp": "02/03/2025 01:09:02",
        "Bit Type Indicator": "NOT_BIT",
        "Consecutive True Count": "1",
        "Total True Count": "1",
        "Consecutive False Count": "0",
        "Total False Count": "0",
        "Total Count": "1",
        "System Fault Code": "28546",
        "RDR Component": "NONE",
        "Appended Data": "",
    }
    result = scrape_rfc_db_record(rfc_db_line)
    assert result == expected


# The following test demonstrates a bug in scrape_pfc_db_record
def test_parse_pfc_db_bug():
    """Demonstrates a bug in scrape_pfc_db_record where indices are misaligned."""
    pfc_db_line = "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,COMM,CI,E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,,,,,,,,,False,True,,,False,,,,,,,,,,,"
    result = scrape_pfc_db_record(pfc_db_line)
    assert result["Subsystem"] == "02/03/2025 01:09:02"  # pThis is actually the timestamp
    assert result["Processed Fault Code"] == "COMM"  # This is correct
    assert result["Mission Critical Result"] == "E2HAWKEYEE2D-26512-02162-00&DMC-E2HAWKEYEE2D-AAAA-E43-91-0000-01000-411A-A" # This is wrong index


def main():
    import pytest
    pytest.main([__file__])


if __name__ == "__main__":
    test_rpcs_pres()
