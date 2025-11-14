from sqlalchemy import Column, Integer, Float, Boolean, String
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.sqltypes import DateTime
from etude_core.db.models import Base
# Assume 'Base' is imported from your canonical location (e.g., models.__init__)
# from models import Base

# Helper for the DATETIME2 variant consistent with the user's example
DATETIME2_MS = DateTime().with_variant(DATETIME2(3), "mssql")


class Rpcs(Base):
    """Corresponds to RPCS: record_type in E2D_RPCS table."""

    __tablename__ = "E2D_RPCS"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    humidity_b = Column("Humidity B", Float)
    secondary_high_pressure = Column("Secondary High Pressure", Float)
    high_temp = Column("High Temp", Float)
    delta_pressure = Column("Delta Pressure", Float)
    humidity_a = Column("Humidity A", Float)
    manifold_pressure = Column("Manifold Pressure", Float)
    primary_high_pressure = Column("Primary High Pressure", Float)


class RpcsPres(Base):
    """Corresponds to RPCS_PRES: record_type in E2D_RPCS_PRES table."""

    __tablename__ = "E2D_RPCS_PRES"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    dataset_time_stamp = Column("Dataset TimeStamp", DATETIME2_MS)

    # Use a dictionary comprehension to generate the repeated columns cleanly
    _primary_high_pressure_cols = {
        f"primary_high_pressure_{i}": Column(f"Primary high pressure ({i})", Float)
        for i in range(1, 11)
    }
    _secondary_high_pressure_cols = {
        f"secondary_high_pressure_{i}": Column(f"Secondary High Pressure ({i})", Float)
        for i in range(1, 11)
    }
    _manifold_pressure_cols = {
        f"manifold_pressure_{i}": Column(f"Manifold Pressure ({i})", Float)
        for i in range(1, 11)
    }

    # Assign generated columns to the class
    locals().update(_primary_high_pressure_cols)
    locals().update(_secondary_high_pressure_cols)
    locals().update(_manifold_pressure_cols)


class NavData(Base):
    """Corresponds to NAV_DATA: record_type in E2D_NAV_DATA table."""

    __tablename__ = "E2D_NAV_DATA"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    nav_mode = Column("Nav Mode", String(50))
    magnetic_variation = Column("Magnetic Variation", Float)
    true_air_speed = Column("True Air Speed", Float)
    calibrated_airspeed = Column("Calibrated Airspeed", Float)
    true_heading = Column("True Heading", Float)
    magnetic_heading = Column("Magnetic Heading", Float)
    vertical_velocity = Column("Vertical Velocity", Float)
    altitude = Column("Altitude", Float)
    altitude_source = Column("Altitude Source", String(8))
    adc_altitude = Column("ADC Altitude", Float)
    ground_speed = Column("Ground Speed", Float)
    os_is_airborne = Column("O/S is Airborne", Boolean)
    wind_speed = Column("WindSpeed", Float)
    wind_direction = Column("WindDirection", Float)
    adc_go = Column("ADC Go", Boolean)
    gps_go = Column("GPS Go", Boolean)
    ins_go = Column("INS Go", Boolean)
    aircraft_navigation_valid = Column("Aircraft Navigation Valid", Boolean)
    relative_navigation_valid = Column("Relative Navigation Valid", Boolean)
    position_valid = Column("Position Valid", Boolean)
    altitude_valid = Column("Altitude Valid", Boolean)
    horizontal_velocity_valid = Column("Horizontal Velocity Valid", Boolean)
    vertical_velocity_valid = Column("Vertical Velocity Valid", Boolean)
    true_heading_valid = Column("True Heading Valid", Boolean)
    calibrated_airspeed_valid = Column("Calibrated Airspeed Valid", Boolean)
    ground_track_valid = Column("Ground Track Valid", Boolean)
    ground_speed_valid = Column("Ground Speed Valid", Boolean)
    aircraft_roll_valid = Column("Aircraft Roll Valid", Boolean)
    aircraft_pitch_valid = Column("Aircraft Pitch Valid", Boolean)
    true_airspeed_valid = Column("True Airspeed Valid", Boolean)
    os_fom = Column("O/S FOM", Integer)
    gps_fom = Column("GPS FOM", Integer)


class RadarState(Base):
    """Corresponds to RDR_STATE: record_type in E2D_RADAR_STATE table."""

    __tablename__ = "E2D_RADAR_STATE"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    rscp_off_switch_state = Column("RSCP_OFF_Switch_State", String)
    rscp_on_switch_state = Column("RSCP_ON_Switch_State", String)
    rscp_stby_switch_state = Column("RSCP_STBY_Switch_State", String)
    rscp_oper_switch_state = Column("RSCP_OPER_Switch_State", String)
    radar_state = Column("Radar_State", String)
    transmitter_power_is_high = Column("Transmitter_Power_is_HIGH", Boolean)
    transmitter_power_is_med = Column("Transmitter_Power_is_MED", Boolean)
    transmitter_power_is_low = Column("Transmitter_Power_is_LOW", Boolean)
    transmitter_power_is_on_deck = Column("Transmitter_Power_is_ON_DECK", Boolean)
    emirs_power_switch_state = Column("EMIRS_Power_Switch_State", String)
    emirs_power_state = Column("EMIRS_Power_State", String)


class RotoScan(Base):
    """Corresponds to ROTOSCAN: record_type in E2D_ROTOSCAN table."""

    __tablename__ = "E2D_ROTOSCAN"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    scan_mode = Column("ScanMode", String)
    scan_rpm = Column("ScanRPM", Float)
    rpm_command = Column("RPM_Command", String)
    scan_time = Column("ScanTime", Float)


class GfcDb(Base):
    """Corresponds to GFC_DB: record_type in E2D_GFC_DB table."""

    __tablename__ = "E2D_GFC_DB"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    time_stamp = Column("Time Stamp", String)
    group_fault_code = Column("Group Fault Code", Integer)
    confirmation_status = Column("Confirmation Status", String)
    group_evaluation_result = Column("Group Evaluation Result", String)
    intermittent_result = Column("Intermittent Result", String)
    transition_count = Column("Transition Count", Integer)
    display_fault_code = Column("Display Fault Code", Integer)
    primary_reference_designator = Column("Primary Reference Designator", String)
    secondary_reference_designator = Column("Secondary Reference Designator", String)


class PfcDb(Base):
    """Corresponds to PFC_DB: record_type in PFC_DB table."""

    __tablename__ = "PFC_DB"

    folder_id = Column("FolderID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    processed_fault_code = Column("Processed Fault Code", Integer)
    fault_description = Column("Fault Description", String)
    subsystem = Column("Subsystem", String)
    mission_critical_result = Column("Mission Critical Result", String)


class RfcDb(Base):
    """Corresponds to RFC_DB: record_type in E2D_RFC_DB table."""

    __tablename__ = "E2D_RFC_DB"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    fci_indicator = Column("FCI Indicator", String)
    raw_fault_code = Column("Raw Fault Code", String)
    fault_status = Column("Fault Status", String)
    time_stamp = Column("TimeStamp", DATETIME2_MS)
    bit_type_indicator = Column("Bit Type Indicator", String)
    consecutive_true_count = Column("Consecutive True Count", Integer)
    total_true_count = Column("Total True Count", Integer)
    consecutive_false_count = Column("Consecutive False Count", Integer)
    total_false_count = Column("Total False Count", Integer)
    total_count = Column("Total Count", Integer)
    system_fault_code = Column("System Fault Code", Integer)
    rdr_component = Column("RDR Component", String)
    appended_data = Column("Appended Data", String)


class LcsTemp(Base):
    """Corresponds to LCS_TEMP: record_type in E2D_LCS_TEMP table."""

    __tablename__ = "E2D_LCS_TEMP"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    lcs_temp_f = Column("LCS Temp F", String)
    lcs_temp_status = Column("LCS Temp Status", String)
    lcs_time = Column("LCS Time", DATETIME2_MS)


class McInDiscr(Base):
    """Corresponds to MC_IN_DISCR: record_type in E2D_MC_IN_DISCR table."""

    __tablename__ = "E2D_MC_IN_DISCR"

    log_id = Column("LogID", Integer, primary_key=True)
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    power_on = Column("Power On", Boolean)
    cooling_air = Column("Cooling Air", Boolean)
    external_temperature_sensor = Column("External Temperature Sensor", Integer)
    internal_temperature_1_sensor = Column("Internal Temperature 1 Sensor", Integer)
    internal_temperature_2_sensor = Column("Internal Temperature 2 Sensor", Integer)
    internal_temperature_3_sensor = Column("Internal Temperature 3 Sensor", Integer)
    external_relative_humidity = Column("External Relative Humidity", Integer)
    dew_point = Column("Dew Point", Integer)
    air_valve_closed = Column("Air Valve Closed", Boolean)
    air_valve_open = Column("Air Valve Open", Boolean)
    air_flow_enabled = Column("Air Flow Enabled", Boolean)
    h_bridge_fault = Column("H Bridge Fault", Boolean)
    pbit_byte_1_dpr_r_fault = Column("PBIT Byte 1 DPR R Fault", Boolean)
    pbit_byte_1_dpr_w_fault = Column("PBIT Byte 1 DPR W Fault", Boolean)
    pbit_byte_1_dpr_wr_fault = Column("PBIT Byte 1 DPR WR Fault", Boolean)
    pbit_byte_1_air_valve_fault = Column("PBIT Byte 1 Air Valve Fault", Boolean)
    pbit_byte_2_ext_h_fault = Column("PBIT Byte 2 EXT H Fault", Boolean)
    pbit_byte_2_ext_t_fault = Column("PBIT Byte 2 EXT T Fault", Boolean)
    pbit_byte_2_valve_pos_fault = Column("PBIT Byte 2 Valve Pos Fault", Boolean)
    pbit_byte_2_opc_fault = Column("PBIT Byte 2 OPC Fault", Boolean)
    pbit_byte_2_int_t1_fault = Column("PBIT Byte 2 INT T1 Fault", Boolean)
    pbit_byte_2_int_t2_fault = Column("PBIT Byte 2 INT T2 Fault", Boolean)
    pbit_byte_2_int_t3_fault = Column("PBIT Byte 2 INT T3 Fault", Boolean)
    pbit_byte_2_nvstore_fault = Column("PBIT Byte 2 NVSTORE Fault", Boolean)
