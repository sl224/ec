import pandas as pd

from e2ude_core.db.models import LcsTemp, NavData, PfcDb, RfcDb, RpcsPres
from e2ude_core.pipelines.parsers.mc_data import parse_mcdata
from e2ude_core.pipelines.parsers.mc_data_scrape import (
    scrape_lcs_temp_record,
    scrape_pfc_db_record,
    scrape_rpcs_pres_record,
    scrape_rfc_db_record,
)


def test_scrape_pfc_db_record_maps_typed_fields():
    line = (
        "5,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,11/07/2023 18:35:33,"
        "COMM,CI,,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,"
        ",,,,,,,,,False,True,,,False,,,,,,,,,,"
    )

    assert scrape_pfc_db_record(line) == [
        "11/07/2023 18:35:33",
        "35537",
        "CT ROUTER - NFS2 INTERFACE DOWN",
        "COMM",
        "CI",
    ]


def test_scrape_rfc_db_record_extracts_expected_fields():
    line = (
        "1,RFC_DB:,,02/03/2025 01:09:02,SCS,28546,CONFIRMED,01:09:02,"
        "NOT_BIT,ConsecTru,1,TotTru,1,ConsecFal,0,TotFal,0,TotCnt,1,28546,"
        "NONE,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
    )

    assert scrape_rfc_db_record(line) == [
        "02/03/2025 01:09:02",
        "SCS",
        "28546",
        "CONFIRMED",
        "02/03/2025 01:09:02",
        "NOT_BIT",
        "1",
        "1",
        "0",
        "0",
        "1",
        "28546",
        "NONE",
        "",
    ]


def test_scrape_lcs_temp_record_combines_date_with_time():
    line = (
        "1,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,"
        ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
    )

    assert scrape_lcs_temp_record(line) == [
        "02/03/2025 01:09:02",
        "65.6",
        "INIT",
        "02/03/2025 01:09:01",
    ]


def test_scrape_rpcs_pres_record_preserves_cleared_slots():
    line = (
        "1,RPCS_PRES:,,10/09/2023 12:59:19,12:59:00,PRI_HI,CLR,CLR,CLR,CLR,"
        "CLR,CLR,CLR,CLR,CLR,CLR,SEC_HI,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,"
        "CLR,MAN_PRE,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,,,,,,,,,,,,,"
    )

    row = scrape_rpcs_pres_record(line)

    assert row[:2] == ["10/09/2023 12:59:19", "10/09/2023 12:59:00"]
    assert len(row) == 32
    assert all(value is None for value in row[2:])


def test_parse_mcdata_builds_clean_model_payloads(tmp_path):
    mcdata_file = tmp_path / "sample_MCData"
    mcdata_file.write_text(
        "\n".join(
            [
                (
                    "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,"
                    "COMM,CI,,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,"
                    ",,,,,,,,,False,True,,,False,,,,,,,,,,"
                ),
                (
                    "2,RFC_DB:,,02/03/2025 01:09:02,SCS,28546,CONFIRMED,01:09:02,"
                    "NOT_BIT,ConsecTru,1,TotTru,1,ConsecFal,0,TotFal,0,TotCnt,1,"
                    "28546,NONE,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
                ),
                (
                    "3,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,"
                    ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
                ),
            ]
        ),
        encoding="utf-8",
    )

    payload = parse_mcdata(mcdata_file)

    assert payload[PfcDb].shape[0] == 1
    assert payload[PfcDb].iloc[0]["Processed Fault Code"] == 35537
    assert payload[PfcDb].iloc[0]["Subsystem"] == "COMM"
    assert payload[PfcDb].iloc[0]["Mission Critical Result"] == "CI"
    assert payload[PfcDb].iloc[0]["System TimeStamp"] == pd.Timestamp(
        "2025-02-03 01:09:02"
    )

    assert payload[RfcDb].shape[0] == 1
    assert payload[RfcDb].iloc[0]["Raw Fault Code"] == "28546"
    assert payload[RfcDb].iloc[0]["TimeStamp"] == pd.Timestamp("2025-02-03 01:09:02")

    assert payload[LcsTemp].shape[0] == 1
    assert payload[LcsTemp].iloc[0]["LCS Time"] == pd.Timestamp("2025-02-03 01:09:01")
    assert payload[NavData].empty


def test_parse_mcdata_minimal_example_documents_handler_contract(tmp_path):
    """
    Example for new handler authors:
    the parser returns a model-keyed payload where present records populate
    only the relevant tables and untouched tables remain empty.
    """
    mcdata_file = tmp_path / "example_MCData"
    mcdata_file.write_text(
        "\n".join(
            [
                (
                    "1,PFC_DB:,,,35537,CT ROUTER - NFS2 INTERFACE DOWN,02/03/2025 01:09:02,"
                    "COMM,CI,,,,CONFIRMED_TRUE,,,,,NO_GRP,,0,False,True,,,,,False,IFPM,"
                    ",,,,,,,,,False,True,,,False,,,,,,,,,,"
                ),
                (
                    "2,LCS_TEMP:,,02/03/2025 01:09:02,65.6,INIT,01:09:01,"
                    ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
                ),
            ]
        ),
        encoding="utf-8",
    )

    payload = parse_mcdata(mcdata_file)

    assert payload[PfcDb].shape[0] == 1
    assert payload[PfcDb].iloc[0]["Processed Fault Code"] == 35537
    assert payload[PfcDb].iloc[0]["Subsystem"] == "COMM"

    assert payload[LcsTemp].shape[0] == 1
    assert payload[LcsTemp].iloc[0]["LCS Time"] == pd.Timestamp("2025-02-03 01:09:01")

    assert payload[RfcDb].empty
    assert payload[NavData].empty


def test_parse_mcdata_handles_all_clear_rpcs_pres_rows(tmp_path):
    mcdata_file = tmp_path / "clr_only_MCData"
    mcdata_file.write_text(
        (
            "1,RPCS_PRES:,,10/09/2023 12:59:19,12:59:00,PRI_HI,CLR,CLR,CLR,CLR,"
            "CLR,CLR,CLR,CLR,CLR,CLR,SEC_HI,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,"
            "CLR,MAN_PRE,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,CLR,,,,,,,,,,,,,"
        ),
        encoding="utf-8",
    )

    payload = parse_mcdata(mcdata_file)
    pressure_columns = [
        col
        for col in payload[RpcsPres].columns
        if col not in {"LineNumber", "System TimeStamp", "Dataset TimeStamp"}
    ]

    assert payload[RpcsPres].shape == (1, 33)
    assert payload[RpcsPres].iloc[0]["System TimeStamp"] == pd.Timestamp(
        "2023-10-09 12:59:19"
    )
    assert payload[RpcsPres].iloc[0]["Dataset TimeStamp"] == pd.Timestamp(
        "2023-10-09 12:59:00"
    )
    assert payload[RpcsPres].iloc[0][pressure_columns].isna().all()
