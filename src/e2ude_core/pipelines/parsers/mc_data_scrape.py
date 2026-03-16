from pandas import DataFrame

"""
Scraping functions for individual record types within an MCData file.
See MC_Maintenance_Data_User_Guide_v2.11.pdf for details.
"""


def scrape_nav_record(text):
    """
    Parses a NAV_DATA line. The field at index 15 is a string of
    boolean flags that must be flattened into separate columns.

    Sample nav_data_line:
    2,NAV_DATA:,,10/02/2018 20:24:21,NORMAL,-0.60,23.00,24.00,5.37,5.380,
    2.69,73.000,NONE,-4.000,7.13,F,15.91,5.432,TTTFTTTTTTTTTTTT,1,1,,,,,,
    ,,,,,,,,,,,,,,,,,,,,,,,
    """

    def _split_filter_line(line):
        return [t for t in line.split(",")[2:] if t and t != "\n"]

    flatten_idx = 15
    row = []
    data = _split_filter_line(text[2:])
    assert len(data[flatten_idx]) == 16
    row.extend(data[:flatten_idx])
    row.extend(char for char in data[flatten_idx])
    row.extend(data[flatten_idx + 1 :])
    return row


def scrape_rpcs_pres_record(text) -> DataFrame:
    def _normalize_pressure_value(token):
        normalized = token.strip()
        str_map = {
            "": None,
            "CLR": None,
            "INV": "-1",
        }
        return str_map.get(normalized.upper(), normalized)

    def _normalize_pressure_block(block_tokens):
        normalized = [_normalize_pressure_value(token) for token in block_tokens[:10]]
        if len(normalized) < 10:
            normalized.extend([None] * (10 - len(normalized)))
        return normalized

    tokens = [token.strip() for token in text.split(",")]
    if len(tokens) < 5:
        return []

    system_time_stamp = tokens[3]
    dataset_time_stamp = tokens[4]
    if system_time_stamp and dataset_time_stamp and " " not in dataset_time_stamp:
        dataset_time_stamp = (
            f"{system_time_stamp.split(' ')[0]} {dataset_time_stamp}"
        )

    try:
        pri_hi_idx = tokens.index("PRI_HI", 5)
        sec_hi_idx = tokens.index("SEC_HI", pri_hi_idx + 1)
        man_pre_idx = tokens.index("MAN_PRE", sec_hi_idx + 1)
    except ValueError:
        return []

    # Preserve placeholder positions so fully-cleared records still match the schema.
    primary_high = _normalize_pressure_block(tokens[pri_hi_idx + 1 : sec_hi_idx])
    secondary_high = _normalize_pressure_block(tokens[sec_hi_idx + 1 : man_pre_idx])
    manifold = _normalize_pressure_block(tokens[man_pre_idx + 1 : man_pre_idx + 11])

    return [system_time_stamp, dataset_time_stamp] + primary_high + secondary_high + manifold


def scrape_pfc_db_record(text):
    """
    Parses a PFC_DB line into the model column order:
    System TimeStamp, Processed Fault Code, Fault Description,
    Subsystem, Mission Critical Result.
    """
    tokens = [token.strip() for token in text.split(",")]
    if len(tokens) < 9:
        return []

    if not tokens[3] and len(tokens) >= 9:
        return [tokens[6], tokens[4], tokens[5], tokens[7], tokens[8]]

    return [tokens[3], tokens[4], tokens[5], tokens[6], tokens[8]]


def scrape_rfc_db_record(text):
    """
    E.g
        "2,RFC_DB:,,10/02/2018 20:24:19,RDR,115,CLEARED,20:24:19,PBIT,
        ConsecTru,0,TotTru,218,ConsecFal,1,TotFal,1,TotCnt,219,21421,
        ADS,1929407492;15;16#0000#;0,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
    """
    tokens = [token.strip() for token in text.split(",")]
    if len(tokens) < 22:
        return []

    return [
        tokens[3],
        tokens[4],
        tokens[5],
        tokens[6],
        tokens[3].split(" ")[0] + " " + tokens[7],
        tokens[8],
        tokens[10],
        tokens[12],
        tokens[14],
        tokens[16],
        tokens[18],
        tokens[19],
        tokens[20],
        tokens[21],
    ]


def scrape_rpcs_record(text):
    """
    Parses an RPCS line, which contains binary-encoded values that must be decoded.
    """

    def decode(arinc, param_name):
        decode_map = {
            "HUM_B": (arinc * 1.26) - 175,
            "SEC_HI_PRE": (arinc * 0.51) - 12.5,
            "HI_TEMP": (arinc * 0.67) + 38,
            "D_PRES": (arinc * 0.05) - 2.5,
            "HUM_A": (arinc * 1.26) - 175,
            "MAN_PRE": (arinc * 0.51) - 12.5,
            "PRI_HI_PRE": (arinc * 0.51) - 12.5,
        }
        if param_name not in decode_map:
            raise ValueError(f"Param name {param_name} not handled")
        return decode_map[param_name]

    row = []
    comma_split_tokens = text.split(",")
    row.append(comma_split_tokens[3])
    binary_start_read_idx = 4
    offset = 14
    for i in range(binary_start_read_idx, binary_start_read_idx + offset, 2):
        token = comma_split_tokens[i]
        bin_data_word = comma_split_tokens[i + 1].split(" ")[2]
        dec = int(bin_data_word, 2)
        param_val = decode(dec, token)
        row.append(round(param_val, 3))
    return row


def scrape_rdr_state_record(text):
    """
    Parses an RDR_STATE line.
    """
    row = []
    stripped = [token for token in text.split(",") if token and token != "\n"]
    assert len(stripped) == 25
    start_idx = 2
    row.extend(stripped[i] for i in range(start_idx, len(stripped), 2))
    return row


def scrape_rotoscan_record(text):
    """
    Parses a ROTOSCAN line.
    """
    row = []
    stripped = [token.strip() for token in text.split(",") if token and token != "\n"]
    row.extend(stripped[2:])
    return row


def scrape_lcs_temp_record(text):
    """
    Parses an LCS_TEMP line.
    """
    row = []
    stripped = [token.strip() for token in text.split(",")]
    keep_tokens = stripped[3:7]
    row.extend(keep_tokens)

    if len(row) >= 4 and row[0] and row[3] and " " not in row[3] and " " in row[0]:
        row[3] = row[0].split(" ")[0] + " " + row[3]

    return row


def scrape_mc_in_discr(text):
    """
    Parses an MC_IN_DISCR line by slicing tokens based on fixed positions.
    """
    text = text.split(",")

    time_stamp = [text[3]]
    ac_stat = text[5:7]
    cab_env = text[8:14]
    airflow = text[15:19]
    byt_0 = text[20:24]
    byt_1 = text[25:33]

    return time_stamp + ac_stat + cab_env + airflow + byt_0 + byt_1
