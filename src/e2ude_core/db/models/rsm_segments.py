from sqlalchemy import Column, Integer, String, ForeignKey
from e2ude_core.db.base_session import Base, schema_fkey, E2UDE_DATETIME


class SegmentsData(Base):
    """
    Represents data parsed from a SEGMENTS file.
    """

    __tablename__ = "rsmdata_segments"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column(Integer, primary_key=True)
    group = Column(Integer)
    event_start = Column(E2UDE_DATETIME(3))
    event_stop = Column(E2UDE_DATETIME(3))
    flight_status = Column(String(100))
