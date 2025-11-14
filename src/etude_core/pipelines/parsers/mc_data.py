import pandas as pd
from collections import defaultdict
from pathlib import Path
from typing import Dict, Type

# --- FIX: Import Base from base_session, not models ---
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

# --- FIX: Import the new vectorized cleaning function ---
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

# --- 2. Update Parser Map ---
# (This map is correct as-is)
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
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    # Create a defaultdict to hold lists of raw row data
    data = defaultdict(list)

    for i, line in enumerate(lines):
        try:
            # The 2nd token is the message type
            tokens = line.split(",", maxsplit=2)
            if len(tokens) < 2:
                continue  # Skip blank lines
            message_type_str = tokens[1]

            if message_type_str in parser_map:
                model, scrape_func = parser_map[message_type_str]

                # 1. Scrape the data-part of the line
                scraped_data = scrape_func(line)

                # 2. Assemble the full row: (line_number) + data columns
                # --- FIX: Your models use 'line_number', not 'log_id' ---
                row = [i] + scraped_data

                data[message_type_str].append(row)
        except Exception:
            # Skip blank or malformed lines
            continue

    ret_payload: Dict[Type[Base], pd.DataFrame] = {}

    for str_key, model_tuple in parser_map.items():
        model, _ = model_tuple

        # --- FIX: This logic was flawed ---

        # 1. Get *all* column names from the model's table
        all_model_cols = [c.name for c in model.__table__.columns]

        # 2. Create the list of columns for the DataFrame
        # We must put LineNumber first, as it's the first item in our 'row'
        columns = []
        if "LineNumber" in all_model_cols:
            columns.append("LineNumber")

        # 3. Add all other non-PK columns
        # (This skips hash_id and LineNumber, which is correct)
        data_cols = [c.name for c in model.__table__.columns if not c.primary_key]
        columns.extend(data_cols)

        # Get the raw data for this model
        raw_rows = data[str_key]

        # Create the raw (string) DataFrame
        # Ensure correct columns, even if no data was found
        df = pd.DataFrame(raw_rows, columns=columns)

        # --- FIX: Clean the DataFrame using the new function ---
        clean_df = clean_dataframe_from_model(df, model)

        ret_payload[model] = clean_df

    return ret_payload
