"""
Scraping functions for individual record types within an MCData file.
Refactored to return dictionaries for Schema independence.
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
    data = _split_filter_line(text[2:])

    assert len(data[flatten_idx]) == 16
    bool_chars = [char for char in data[flatten_idx]]

    values = data[:flatten_idx] + bool_chars + data[flatten_idx + 1 :]

    keys = [
        "Nav Mode",
        "Magnetic Variation",
        "True Air Speed",
        "Calibrated Airspeed",
        "True Heading",
        "Magnetic Heading",
        "Vertical Velocity",
        "Altitude",
        "Altitude Source",
        "ADC Altitude",
        "Ground Speed",
        "O/S is Airborne",
        "WindSpeed",
        "WindDirection",
        "ADC Go",
        "GPS Go",
        "INS Go",
        "Aircraft Navigation Valid",
        "Relative Navigation Valid",
        "Position Valid",
        "Altitude Valid",
        "Horizontal Velocity Valid",
        "Vertical Velocity Valid",
        "True Heading Valid",
        "Calibrated Airspeed Valid",
        "Ground Track Valid",
        "Ground Speed Valid",
        "Aircraft Roll Valid",
        "Aircraft Pitch Valid",
        "True Airspeed Valid",
        "O/S FOM",
        "GPS FOM",
    ]

    full_tokens = text.split(",")
    system_time = full_tokens[3]

    row_dict = dict(zip(keys, values))
    row_dict["System TimeStamp"] = system_time
    return row_dict


def scrape_rpcs_pres_record(text) -> dict:
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

    data_tokens = text.split(",")[2:]
    fil_row = [_filter_rpcs(token) for token in data_tokens]
    clean_values = [token for token in fil_row if token]

    keys = ["System TimeStamp", "Dataset TimeStamp"]
    for i in range(1, 11):
        keys.append(f"Primary high pressure ({i})")
        keys.append(f"Secondary High Pressure ({i})")
        keys.append(f"Manifold Pressure ({i})")

    return dict(zip(keys, clean_values))


def scrape_pfc_db_record(text):
    tokens = text.split(",")
    return {
        "System TimeStamp": tokens[3],
        "Processed Fault Code": tokens[6],
        "Fault Description": tokens[4],
        "Subsystem": tokens[5],
        "Mission Critical Result": tokens[8],
    }


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
    tokens = text.split(",")

    row = {
        "System TimeStamp": tokens[3],
        "FCI Indicator": tokens[4],
        "Raw Fault Code": tokens[5],
        "Fault Status": tokens[6],
        "TimeStamp": tokens[7],
        "Bit Type Indicator": tokens[8],
        "Consecutive True Count": tokens[10],
        "Total True Count": tokens[12],
        "Consecutive False Count": tokens[14],
        "Total False Count": tokens[16],
        "Total Count": tokens[18],
        "System Fault Code": tokens[19],
        "RDR Component": tokens[20],
        "Appended Data": tokens[21],
    }

    date_part = tokens[3].split(" ")[0]
    row["TimeStamp"] = f"{date_part} {tokens[7]}"

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
        return decode_map.get(param_name, 0.0)

    row = {}
    comma_split_tokens = text.split(",")
    row["System TimeStamp"] = comma_split_tokens[3]

    param_to_col = {
        "HUM_B": "Humidity B",
        "SEC_HI_PRE": "Secondary High Pressure",
        "HI_TEMP": "High Temp",
        "D_PRES": "Delta Pressure",
        "HUM_A": "Humidity A",
        "MAN_PRE": "Manifold Pressure",
        "PRI_HI_PRE": "Primary High Pressure",
    }

    BINARY_START_READ_IDX = 4
    # Number of tokens to traverse (7 pairs of param_name, binary_value)
    OFFSET = 14
    for i in range(BINARY_START_READ_IDX, BINARY_START_READ_IDX + OFFSET, 2):
        token = comma_split_tokens[i]
        bin_data_word = comma_split_tokens[i + 1].split(" ")[2]
        dec = int(bin_data_word, 2)
        param_val = decode(dec, token)

        col_name = param_to_col.get(token)
        if col_name:
            row[col_name] = round(param_val, 3)

    return row


def scrape_rdr_state_record(text):
    """
    Parses an RDR_STATE line.
    """
    stripped = [token for token in text.split(",") if token and token != "\n"]
    keys = [
        "System TimeStamp",
        "RSCP_OFF_Switch_State",
        "RSCP_ON_Switch_State",
        "RSCP_STBY_Switch_State",
        "RSCP_OPER_Switch_State",
        "Radar_State",
        "Transmitter_Power_is_HIGH",
        "Transmitter_Power_is_MED",
        "Transmitter_Power_is_LOW",
        "Transmitter_Power_is_ON_DECK",
        "EMIRS_Power_Switch_State",
        "EMIRS_Power_State",
    ]
    values = [stripped[i] for i in range(2, len(stripped), 2)]
    return dict(zip(keys, values))


def scrape_rotoscan_record(text):
    """
    Parses a ROTOSCAN line.
    """
    stripped = [token.strip() for token in text.split(",") if token and token != "\n"]
    keys = ["System TimeStamp", "ScanMode", "ScanRPM", "RPM_Command", "ScanTime"]
    return dict(zip(keys, stripped[2:]))


def scrape_lcs_temp_record(text: str):
    """
    Parses an LCS_TEMP line.
    """
    stripped = [token.strip() for token in text.split(",")]
    keys = ["System TimeStamp", "LCS Temp F", "LCS Temp Status", "LCS Time"]
    return dict(zip(keys, stripped[3:7]))


def scrape_mc_in_discr(text):
    """
    Parses an MC_IN_DISCR line by slicing tokens based on fixed positions.
    """
    text = text.split(",")
    values = (
        [text[3]] + text[5:7] + text[8:14] + text[15:19] + text[20:24] + text[25:33]
    )

    keys = [
        "System TimeStamp",
        "Power On",
        "Cooling Air",
        "External Temperature Sensor",
        "Internal Temperature 1 Sensor",
        "Internal Temperature 2 Sensor",
        "Internal Temperature 3 Sensor",
        "External Relative Humidity",
        "Dew Point",
        "Air Valve Closed",
        "Air Valve Open",
        "Air Flow Enabled",
        "H Bridge Fault",
        "PBIT Byte 1 DPR R Fault",
        "PBIT Byte 1 DPR W Fault",
        "PBIT Byte 1 DPR WR Fault",
        "PBIT Byte 1 Air Valve Fault",
        "PBIT Byte 2 EXT H Fault",
        "PBIT Byte 2 EXT T Fault",
        "PBIT Byte 2 Valve Pos Fault",
        "PBIT Byte 2 OPC Fault",
        "PBIT Byte 2 INT T1 Fault",
        "PBIT Byte 2 INT T2 Fault",
        "PBIT Byte 2 INT T3 Fault",
        "PBIT Byte 2 NVSTORE Fault",
    ]

    return dict(zip(keys, values))
