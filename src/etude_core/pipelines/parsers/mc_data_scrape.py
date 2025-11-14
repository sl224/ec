from pandas import DataFrame

"""
Contains scraping strategies for the MCData file records
See MC_Maintenance_Data_User_Guide_v2.11.pdf for data details
"""


def scrape_nav_record(text):
    """
    Parses a single NAV_DATA line record
    The data field at index 15 of a data_tokens line is a character string
    of booleans that need to be flattened

    Sample nav_data_line:
    2,NAV_DATA:,,10/02/2018 20:24:21,NORMAL,-0.60,23.00,24.00,5.37,5.380,
    2.69,73.000,NONE,-4.000,7.13,F,15.91,5.432,TTTFTTTTTTTTTTTT,1,1,,,,,,
    ,,,,,,,,,,,,,,,,,,,,,,,
    """

    def _split_filter_line(line):
        return [t for t in line.split(",")[2:] if t and t != "\n"]

    flatten_idx = 15
    row = []
    # Data starts from index 2 on
    data = _split_filter_line(text[2:])
    assert len(data[flatten_idx]) == 16
    row.extend(data[:flatten_idx])
    row.extend(char for char in data[flatten_idx])
    row.extend(data[flatten_idx + 1 :])
    return row


def scrape_rpcs_pres_record(text) -> DataFrame:
    def _filter_rpcs(token):
        # Range of Pressure vals defined in the interface design document (IDD)
        # E.g (0.0..99999999.9,CLR,INV)
        # We transform to None and -1 as to fit the float datatype
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
    E.g:
        "2,PFC_DB:,,,28421,RDR CH I/Q FAULT-CH 15 1929407492P,
        10/02/2018 20:24:28,RADAR,NCI,,,,CONFIRMED_FALSE,PBIT,
        39A4A1,,,NO_GRP,,1,false,true,,,,,false,MAINT,,,,,,,,,
        false,TRUE,,,true,,,,,,,,,"

    Data Fields:
    1. Time Stamp � Time of fault processing.
    2. Processed Fault Code � Numeric code.
    3. Description: Str
    4. Subsystem: Str
    5. Mission Critical Result � Criticality of the fault.

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
    # assert len(data_tokens) == 13
    # print('data_tokens', data_tokens)
    row.extend(data_tokens)
    row[6] = row[2].split(" ")[0] + " " + row[6]
    return row


def scrape_rpcs_record(text):
    """
    E.g:

    Data Fields:
    1. Time Stamp � Time that parameters were recorded.
    2. Humidity Sensor B � Reported Humidity.
    3. Secondary High Pressure Sensor � Reported Pressure.
    4. High Temperature Sensor � Reported Temperature
    5. Delta Pressure Sensor � Reported Pressure.
    6. Humidity Sensor A � Reported Humidity.
    7. Manifold Pressure � Reported Pressure.
    8. Primary High Pressure � Reported Pressure
    """

    def decode(ARINC, param_name):
        """
        Decodes the binary values from the RPCS record in the MCData files
        Constants are defined in the rpcs_parser.java
        The java file was sent via email from Dominick Terrasi
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

    # Append LogID, LineNumber
    row = []
    comma_split_tokens = text.split(",")
    # Append datetime
    row.append(comma_split_tokens[3])
    BINARY_START_READ_IDX = 4
    # Number of binary chars to traverse
    OFFSET = 14
    for i in range(BINARY_START_READ_IDX, BINARY_START_READ_IDX + OFFSET, 2):
        token = comma_split_tokens[i]
        bin_data_word = comma_split_tokens[i + 1].split(" ")[2]
        dec = int(bin_data_word, 2)
        param_val = decode(dec, token)
        row.append(round(param_val, 3))
    return row


def scrape_rdr_state_record(text):
    """ """
    row = []
    stripped = [token for token in text.split(",") if token and token != "\n"]
    assert len(stripped) == 25
    START_IDX = 2
    row.extend(stripped[i] for i in range(START_IDX, len(stripped), 2))
    return row


def scrape_rotoscan_record(text):
    """ """
    row = []
    stripped = [token.strip() for token in text.split(",") if token and token != "\n"]
    row.extend(stripped[2:])
    return row


def scrape_lcs_temp_record(text: str):
    """
    E.g:
        "2,LCS_TEMP:,,12/02/2021 14:32:27,72.7,VALID,14:32:27
         ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"

     Data Fields:
        1. Time Stamp: System Temp
        2. LCS Temperature: degrees Fahrenheit
        3. LCS Temp Status
            INIT : Initial state
            VALID : normal processing
            STALE : the data was not updated by MUX FCI within the last 10
                    seconds.
        4. LCS Time: the time stamp when MUX FCI last recorded the
                     LCS temperature
    """
    row = []
    stripped = [token.strip() for token in text.split(",")]
    keep_tokens = stripped[3:7]
    row.extend(keep_tokens)
    return row


def scrape_mc_in_discr(text):
    """
    E.g.
        query:

        "SELECT [RawText] FROM [LcdDataMart].[dbo].[E2D_LogRecords]
        WHERE [RawText] LIKE '%MC_In_DISCR%'"

        Sample Line:
        "1,MC_IN_DISCR:,,09/27/2018 11:35:21,AC_Stat,T,T,MC_CabEnv,14,23,
        15,20,49,-6,MC_Airflow,F,T,F,F,PBT_Byt0,F,F,F,F,PBT_Byt1,F,F,F,F,F,F,F
        ,F,,,,,,,,,,,,,,,,,"

    Data Fields:
        MC_IN_DISCR
        ===========
        01. Time Stamp - datetime

        AC Stat
        =======
        02. MC Power On - bool
        03. MC Cooling Air - bool

        MC_CabEnv
        =========
        04. MC External Temperature Sensor - int
        05. MC Internal Temperature Sensor 1 - int
        06. MC Internal Temperature Sensor 2 - int
        07. MC Internal Temperature Sensor 3 - int
        08. MC External Relative Humidity - int
        09. MC Dew Point - int

        MC_Airflow
        ==========
        10. MC Air Valve Closed - bool
        11. MC Air Valve Open - bool
        12. MC Air Flow Enabled - bool
        13. MC H Bridge Fault - bool

        PBT_Byt0
        ========
        14. MC PBIT Byte 1 DPR R Fault - bool
        15. MC PBIT Byte 1 DPR W Fault - bool
        16. MC PBIT Byte 1 DPR WR Fault - bool
        17. MC PBIT Byte 1 Air Valve Fault - bool

        PBT_Byt1
        ========
        18. MC PBIT Byte 2 EXT H Fault - bool
        19. MC PBIT Byte 2 EXT T Fault - bool
        20. MC PBIT Byte 2 Valve Pos Fault - bool
        21. MC PBIT Byte 2 OPC Fault - bool
        22. MC PBIT Byte 2 INT T1 Fault - bool
        23. MC PBIT Byte 2 INT T2 Fault - bool
        24. MC PBIT Byte 2 INT T3 Fault - bool
        25. MC PBIT Byte 2 NVSTORE Fault - bool
    """
    # tokenize
    text = text.split(",")

    # organize
    time_stamp = [text[3]]
    ac_stat = text[5:7]
    cab_env = text[8:14]
    airflow = text[15:19]
    byt_0 = text[20:24]
    byt_1 = text[25:33]

    # append
    return time_stamp + ac_stat + cab_env + airflow + byt_0 + byt_1
