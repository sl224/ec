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

test_file = Path(r"tests/static_assets/zips/169069_20250203_004745_025_TransportRSM.fpkg.e2d/169069_20250203_004745_025_MCData")
with open(test_file, 'r') as f:
    lines = f.readlines()


data = defaultdict(list)
for line in lines:
    message_type_str = line.split(',', maxsplit=2)[1]
    if message_type_str in parser_map:
        _, scrape_func = parser_map[message_type_str]
        data[message_type_str].append(scrape_func(line))

ret_payload = {}
for k in parser_map:
    model = parser_map[k][0]
    columns = [c.name for c in model.__table__.columns if not c.primary_key]
    df = pd.DataFrame(data[k], columns=columns)
    ret_payload[k] = df

return ret_payload
