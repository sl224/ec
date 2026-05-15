from sqlalchemy import Column, Integer, String, VARBINARY
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.sqltypes import DateTime

from e2ude_core.db.base_session import Base


# DATETIME2 variant for MSSQL compatibility.
DATETIME2_MS = DateTime().with_variant(DATETIME2(0), "mssql")


class EngineOnOff(Base):
    __tablename__ = "rsmdata_engine_on_off"

    content_hash = Column(VARBINARY(16), primary_key=True)
    line_number = Column(Integer, primary_key=True)

    engine_position = Column("engine_position", String)
    start_time = Column("start_time", DATETIME2_MS)
    end_time = Column("stop_time", DATETIME2_MS)
    run_time_seconds = Column("run_time_seconds", Integer)
    segment_number = Column("segment_number", Integer)
    ifpm_version = Column("ifpm_version", String)
