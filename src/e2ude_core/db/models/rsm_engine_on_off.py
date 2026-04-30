from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.sql.sqltypes import DateTime

# Import Base AND the new schema_fkey helper
from e2ude_core.db.base_session import Base, schema_fkey


# DATETIME2 variant for MSSQL compatibility.
DATETIME2_MS = DateTime().with_variant(DATETIME2(0), "mssql")


class EngineOnOff(Base):
    __tablename__ = "rsmdata_engine_on_off5"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column(Integer, primary_key=True)

    engine_position = Column("engine_position", String)
    start_time = Column("start_time", DATETIME2_MS)
    end_time = Column("stop_time", DATETIME2_MS)
    run_time = Column("run_time", DATETIME2_MS)
    # start_time = Column("start_time", DateTime)
    # stop_time = Column("stop_time", DateTime)
    # run_time = Column("run_time", DateTime)
    segment_number = Column("segment_number", Integer)
    ifpm_version = Column("ifpm_version", String)
