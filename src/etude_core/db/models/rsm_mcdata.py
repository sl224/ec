from sqlalchemy import Column, Integer, Float, Boolean, String, ForeignKey
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.sqltypes import DateTime

# Import Base AND the new schema_fkey helper
from etude_core.db.base_session import Base, schema_fkey


# DATETIME2 variant for MSSQL compatibility.
DATETIME2_MS = DateTime().with_variant(DATETIME2(0), "mssql")


class Rpcs(Base):
    __tablename__ = "rsmdata_mc_rpcs"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
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
    __tablename__ = "rsmdata_mc_rpcs_pres"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    dataset_time_stamp = Column("Dataset TimeStamp", DATETIME2_MS)

    # Generate repeated pressure columns programmatically.
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

    # Add generated columns to the class namespace.
    locals().update(_primary_high_pressure_cols)
    locals().update(_secondary_high_pressure_cols)
    locals().update(_manifold_pressure_cols)


class NavData(Base):
    __tablename__ = "rsmdata_mc_nav_data"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
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
    __tablename__ = "rsmdata_mc_radar_state"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
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
    __tablename__ = "rsmdata_mc_rotoscan"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    scan_mode = Column("ScanMode", String)
    scan_rpm = Column("ScanRPM", Float)
    rpm_command = Column("RPM_Command", String)

    scan_time = Column("ScanTime", Float)


class GfcDb(Base):
    __tablename__ = "rsmdata_mc_gfc_db"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
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
    __tablename__ = "rsmdata_mc_pfc_db"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    processed_fault_code = Column("Processed Fault Code", Integer)
    fault_description = Column("Fault Description", String)
    subsystem = Column("Subsystem", String)
    mission_critical_result = Column("Mission Critical Result", String)


class RfcDb(Base):
    __tablename__ = "rsmdata_mc_rfc_db"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
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
    __tablename__ = "rsmdata_mc_lcs_temp"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column("LineNumber", Integer, primary_key=True)
    system_time_stamp = Column("System TimeStamp", DATETIME2_MS)
    lcs_temp_f = Column("LCS Temp F", String)
    lcs_temp_status = Column("LCS Temp Status", String)
    lcs_time = Column("LCS Time", DATETIME2_MS)


class McInDiscr(Base):
    __tablename__ = "rsmdata_mc_mc_in_discr"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
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
