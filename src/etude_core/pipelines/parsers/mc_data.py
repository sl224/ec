import pandas as pd
from collections import defaultdict
from pathlib import Path
from typing import Dict, Type

# Assuming Base is in base_session
from etude_core.db.base_session import Base

# Import the models this parser produces
from etude_core.db.models import (
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

# Import the new cleaning helper
from etude_core.pipelines.cleaning import clean_dataframe_from_model

# Import the scrape functions
from etude_core.pipelines.parsers.mc_data_scrape import (
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

# This map links the file's string key to the Model and Scrape function
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
    Parses an MCData file, scrapes each line, builds DataFrames,
    and cleans them according to their SQLAlchemy model definitions.
    """
    with open(file_path, "r") as f:
        lines = f.readlines()

    data = defaultdict(list)

    for i, line in enumerate(lines):
        try:
            message_type_str = line.split(",", maxsplit=2)[1]

            if message_type_str in parser_map:
                _, scrape_func = parser_map[message_type_str]

                row = [i]
                row.extend(scrape_func(line))

                data[message_type_str].append(row)
        except IndexError:
            continue

    ret_payload: Dict[Type[Base], pd.DataFrame] = {}

    for str_key, model_tuple in parser_map.items():
        model, _ = model_tuple
        columns = ["line_number"]
        model_cols = [c.name for c in model.__table__.columns if not c.primary_key]
        columns.extend(model_cols)
        raw_rows = data[str_key]
        df = pd.DataFrame(raw_rows, columns=columns)
        clean_df = clean_dataframe_from_model(df, model)
        ret_payload[model] = clean_df

    return ret_payload
