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
    # Data starts from the 3rd comma-separated token.
    data = _split_filter_line(text[2:])
    assert len(data[flatten_idx]) == 16
    row.extend(data[:flatten_idx])
    row.extend(char for char in data[flatten_idx])
    row.extend(data[flatten_idx + 1 :])
    return row


def scrape_rpcs_pres_record(text) -> DataFrame:
    def _filter_rpcs(token):
        # Map non-numeric pressure values from the spec to values that
        # can be coerced into a float column (None for NULL, -1 for INV).
        # E.g., (0.0..99999999.9, CLR, INV)
        str_map = {
            "CLR": None,
            "INV": "-1",
            "PRI_HI": "",
            "SEC_HI": "",
            "MAN_PRE": "",
            "\n": "",
        }
        if token.upper() in str_map:
            return str_map[token]
        return token

    row = []
    data_tokens = text.split(",")[2:]
    fil_row = [_filter_rpcs(token) for token in data_tokens]
    row.extend(token for token in fil_row if token)
    return row


def scrape_pfc_db_record(text):
    """
    Parses a PFC_DB line.
    """
    row = []
    tokens = text.split(",")
    row.append(tokens[6])
    keep_tokens = tokens[4:9]
    ignore_idx = {2}
    data_tokens = [token for i, token in enumerate(keep_tokens) if i not in ignore_idx]

    row.extend(data_tokens)
    return row


def scrape_rfc_db_record(text):
    """
    E.g
        "2,RFC_DB:,,10/02/2018 20:24:19,RDR,115,CLEARED,20:24:19,PBIT,
        ConsecTru,0,TotTru,218,ConsecFal,1,TotFal,1,TotCnt,219,21421,
        ADS,1929407492;15;16#0000#;0,,,,,,,,,,,,,,,,,,,,,,,,,,,,"

    Data Fields:
    1. FCI Indicator � Indicator of the FCI reporting the fault.
    2. Raw Fault Code � Numeric code.
    3. Fault Status � �CONFIRMED� or �CLEARED� status.
    4. Time Stamp � Time of last report of fault.
    5. Bit Type Indicator � �IBIT�, �SBIT�, �PBIT�, or �NOT_BIT� indicator.
    6. Consecutive True Count � Number of consecutive reports
        received with fault set to True.
    7. Total True Count � Number of reports received with fault set to true.
    8. Consecutive False Count � Number of consecutive reports received
        with fault set to False.
    9. Total False Count � Number of reports received with fault set to false.
    10. Total Count � Total reports received containing status of fault.
    11. System Fault Code � Numeric code assigned by DIA FCI
    12. RDR Component � Indication of which Radar processor �
        �NONE�, �ADS�, �TARA�
    13. Appended Data � Additional data added to record as needed.

    """
    row = []
    ignore_idxs = {5, 7, 9, 11, 13}
    tokens = text.split(",")
    row.append(tokens[3])
    keep_tokens = tokens[4:22]
    # print('keep_tokens', keep_tokens)
    data_tokens = [token for i, token in enumerate(keep_tokens) if i not in ignore_idxs]
    row.extend(data_tokens)
    row[6] = row[2].split(" ")[0] + " " + row[6]
    return row


def scrape_rpcs_record(text):
    """
    Parses an RPCS line, which contains binary-encoded values that must be decoded.
    """

    def decode(ARINC, param_name):
        """
        Decodes binary values from the RPCS record.
        Constants are derived from the original rpcs_parser.java implementation.
        """
        decode_map = {
            "HUM_B": (ARINC * 1.26) - 175,
            "SEC_HI_PRE": (ARINC * 0.51) - 12.5,
            "HI_TEMP": (ARINC * 0.67) + 38,
            "D_PRES": (ARINC * 0.05) - 2.5,
            "HUM_A": (ARINC * 1.26) - 175,
            "MAN_PRE": (ARINC * 0.51) - 12.5,
            "PRI_HI_PRE": (ARINC * 0.51) - 12.5,
        }
        if param_name not in decode_map:
            raise ValueError(f"Param name {param_name} not handled")
        return decode_map[param_name]

    row = []
    comma_split_tokens = text.split(",")
    row.append(comma_split_tokens[3])  # datetime
    BINARY_START_READ_IDX = 4
    # Number of tokens to traverse (7 pairs of param_name, binary_value)
    OFFSET = 14
    for i in range(BINARY_START_READ_IDX, BINARY_START_READ_IDX + OFFSET, 2):
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
    START_IDX = 2
    row.extend(stripped[i] for i in range(START_IDX, len(stripped), 2))
    return row


def scrape_rotoscan_record(text):
    """
    Parses a ROTOSCAN line.
    """
    row = []
    stripped = [token.strip() for token in text.split(",") if token and token != "\n"]
    row.extend(stripped[2:])
    return row


def scrape_lcs_temp_record(text: str):
    """
    Parses an LCS_TEMP line.
    """
    row = []
    stripped = [token.strip() for token in text.split(",")]
    keep_tokens = stripped[3:7]
    row.extend(keep_tokens)
    return row


def scrape_mc_in_discr(text):
    """
    Parses an MC_IN_DISCR line by slicing tokens based on fixed positions.
    """
    text = text.split(",")

    # Extract data segments by known index slices
    time_stamp = [text[3]]
    ac_stat = text[5:7]
    cab_env = text[8:14]
    airflow = text[15:19]
    byt_0 = text[20:24]
    byt_1 = text[25:33]

    # Combine and return all segments
    return time_stamp + ac_stat + cab_env + airflow + byt_0 + byt_1
