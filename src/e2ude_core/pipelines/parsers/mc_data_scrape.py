"""
Scraping functions for individual record types within an MCData file.
"""

from typing import Dict, Any


def scrape_nav_record(text: str) -> Dict[str, Any]:
    """
    Parses a NAV_DATA line.
    FIXED: Uses index-based splitting to preserve empty columns (preventing data shift).
    """
    # 1. Raw Split (Preserve empty strings to maintain column alignment)
    raw_tokens = text.split(",")

    # 2. Validation (Need at least up to the boolean string at index 18)
    if len(raw_tokens) < 19:
        return {}

    # 3. Extract System Time (Index 3)
    system_time = raw_tokens[3]

    # 4. Extract Scalars (Indices 4 to 17)
    # Nav Mode, Mag Var, TAS, CAS, TH, MH, VV, Alt, AltSrc, ADCAlt, GS, Airborne, WindSpd, WindDir
    # Clean whitespace and convert empty strings to None
    data_start_idx = 4
    bool_string_idx = 18

    scalars = [
        t.strip() if t.strip() else None
        for t in raw_tokens[data_start_idx:bool_string_idx]
    ]

    # 5. Flatten Boolean String (Index 18)
    bool_str = raw_tokens[bool_string_idx].strip()
    if bool_str:
        bool_chars = list(bool_str)
    else:
        # Fallback if missing (though unlikely if length check passed)
        bool_chars = [None] * 16

    # 6. Extract Remaining Data (Indices 19+)
    # FOMs, etc.
    remaining = [
        t.strip() if t.strip() else None for t in raw_tokens[bool_string_idx + 1 :]
    ]

    # 7. Combine Values
    values = scalars + bool_chars + remaining

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

    # Safety: Trim values if line has extra garbage, or pad if short
    if len(values) > len(keys):
        values = values[: len(keys)]
    elif len(values) < len(keys):
        values += [None] * (len(keys) - len(values))

    row_dict = dict(zip(keys, values))
    row_dict["System TimeStamp"] = system_time
    return row_dict


def scrape_rpcs_pres_record(text: str) -> Dict[str, Any]:
    def _filter_rpcs(token):
        t = token.strip().upper()
        if t in {"", "CLR", "PRI_HI", "SEC_HI", "MAN_PRE", "\n"}:
            return None
        if t == "INV":
            return "-1"
        return t

    # Use raw split to be safe, but filter garbage values
    tokens = text.split(",")
    if len(tokens) < 4:
        return {}

    clean_values = [_filter_rpcs(t) for t in tokens[2:]]  # Skip ID, Type
    # Remove Nones that were filtered out (legacy behavior for RPCS_PRES seems to rely on packing)
    # However, safer to match keys.
    # The original logic: `clean_values = [token for token in fil_row if token]`
    # We will stick to that to avoid breaking legacy RPCS behavior which is sparse.
    clean_values = [v for v in clean_values if v is not None]

    keys = ["System TimeStamp", "Dataset TimeStamp"]
    for i in range(1, 11):
        keys.append(f"Primary high pressure ({i})")
        keys.append(f"Secondary High Pressure ({i})")
        keys.append(f"Manifold Pressure ({i})")

    # Pad if short
    if len(clean_values) < len(keys):
        clean_values += [None] * (len(keys) - len(clean_values))

    return dict(zip(keys, clean_values))


def scrape_pfc_db_record(text: str) -> Dict[str, Any]:
    tokens = text.split(",")
    if len(tokens) < 9:
        return {}
    return {
        "System TimeStamp": tokens[3],
        "Processed Fault Code": tokens[6],
        "Fault Description": tokens[4],
        "Subsystem": tokens[5],
        "Mission Critical Result": tokens[8],
    }


def scrape_rfc_db_record(text: str) -> Dict[str, Any]:
    tokens = text.split(",")
    if len(tokens) < 22:
        return {}

    date_part = tokens[3].split(" ")[0] if " " in tokens[3] else tokens[3]

    return {
        "System TimeStamp": tokens[3],
        "FCI Indicator": tokens[4],
        "Raw Fault Code": tokens[5],
        "Fault Status": tokens[6],
        "TimeStamp": f"{date_part} {tokens[7]}",
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


def scrape_rpcs_record(text: str) -> Dict[str, Any]:
    def decode(ARINC, param_name):
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
    if len(comma_split_tokens) < 4:
        return {}

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
    OFFSET = 14
    try:
        for i in range(BINARY_START_READ_IDX, BINARY_START_READ_IDX + OFFSET, 2):
            if i + 1 >= len(comma_split_tokens):
                break
            token = comma_split_tokens[i]
            bin_token = comma_split_tokens[i + 1]
            if " " in bin_token:
                bin_data_word = bin_token.split(" ")[2]
                dec = int(bin_data_word, 2)
                param_val = decode(dec, token)

                col_name = param_to_col.get(token)
                if col_name:
                    row[col_name] = round(param_val, 3)
    except Exception:
        pass

    return row


def scrape_rdr_state_record(text: str) -> Dict[str, Any]:
    # Safe split
    stripped = [token.strip() for token in text.split(",")]
    # Values are at indices 3, 5, 7... (Skip ID, Type, Empty)
    # Indices: 3 (SysTime), 5, 7, 9...
    # Keys from original:
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

    values = []
    # SysTime is at 3
    sys_ts = stripped[3] if len(stripped) > 3 else None
    values.append(sys_ts)

    # Other values start at 5, step 2
    # We need 11 more values
    for i in range(5, 5 + 22, 2):
        if i < len(stripped):
            val = stripped[i]
            values.append(val if val else None)
        else:
            values.append(None)

    return dict(zip(keys, values))


def scrape_rotoscan_record(text: str) -> Dict[str, Any]:
    stripped = [token.strip() for token in text.split(",")]
    keys = ["System TimeStamp", "ScanMode", "ScanRPM", "RPM_Command", "ScanTime"]
    # 0=ID, 1=Type, 2=SysTime?
    # Original logic: `stripped[2:]`.
    # If file is `2,ROTOSCAN,Timestamp...` -> `stripped[2]` is TS.
    vals = stripped[2:]
    if len(vals) < len(keys):
        vals += [None] * (len(keys) - len(vals))
    return dict(zip(keys, vals))


def scrape_lcs_temp_record(text: str) -> Dict[str, Any]:
    stripped = [token.strip() for token in text.split(",")]
    keys = ["System TimeStamp", "LCS Temp F", "LCS Temp Status", "LCS Time"]
    # Original logic: `stripped[3:7]`
    vals = stripped[3:7]
    if len(vals) < len(keys):
        vals += [None] * (len(keys) - len(vals))
    return dict(zip(keys, vals))


def scrape_mc_in_discr(text: str) -> Dict[str, Any]:
    t = text.split(",")
    if len(t) < 33:
        return {}

    values = [t[3]] + t[5:7] + t[8:14] + t[15:19] + t[20:24] + t[25:33]
    values = [v.strip() if v.strip() else None for v in values]

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
