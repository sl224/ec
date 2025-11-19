import pandas as pd
from collections import defaultdict
from pathlib import Path
from typing import Dict, Type

from e2ude_core.db.base_session import Base

# Import the models this parser produces
from e2ude_core.db.models import (
    Rpcs,
    RpcsPres,
    NavData,
    RadarState,
    RotoScan,
    PfcDb,
    RfcDb,
    LcsTemp,
    McInDiscr,
)
from e2ude_core.pipelines.cleaning import clean_dataframe_from_model
from e2ude_core.pipelines.parsers.mc_data_scrape import (
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

# Maps message type strings to a model and scrape function.
parser_map = {
    "RPCS:": (Rpcs, scrape_rpcs_record),
    "RPCS_PRES:": (RpcsPres, scrape_rpcs_pres_record),
    "NAV_DATA:": (NavData, scrape_nav_record),
    "RDR_STATE:": (RadarState, scrape_rdr_state_record),
    "ROTOSCAN:": (RotoScan, scrape_rotoscan_record),
    "PFC_DB:": (PfcDb, scrape_pfc_db_record),
    "RFC_DB:": (RfcDb, scrape_rfc_db_record),
    "LCS_TEMP:": (LcsTemp, scrape_lcs_temp_record),
    "MC_IN_DISCR:": (McInDiscr, scrape_mc_in_discr),
}


def parse_mcdata(file_path: Path) -> Dict[Type[Base], pd.DataFrame]:
    """
    Parses an MCData file, scrapes each line, and builds cleaned DataFrames
    for each message type.
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    data = defaultdict(list)

    for i, line in enumerate(lines):
        try:
            tokens = line.split(",", maxsplit=2)
            if len(tokens) < 2:
                continue  # Skip blank lines
            message_type_str = tokens[1]

            if message_type_str in parser_map:
                model, scrape_func = parser_map[message_type_str]

                row_dict = scrape_func(line)
                row_dict["LineNumber"] = i

                data[message_type_str].append(row_dict)
        except Exception:
            continue

    ret_payload: Dict[Type[Base], pd.DataFrame] = {}

    for str_key, model_tuple in parser_map.items():
        model, _ = model_tuple
        raw_rows = data[str_key]

        df = pd.DataFrame(raw_rows)

        if not df.empty:
            clean_df = clean_dataframe_from_model(df, model)
            ret_payload[model] = clean_df
        else:
            # Create an empty DataFrame with correct columns if no data was found.
            cols = [c.name for c in model.__table__.columns]
            cols_to_keep = [c for c in cols if c not in ("id", "hash_id")]
            ret_payload[model] = pd.DataFrame(columns=cols_to_keep)

    return ret_payload
