from sqlalchemy import Column, Integer, String, VARBINARY
from e2ude_core.db.base_session import Base, E2UDE_DATETIME


class SegmentsData(Base):
    """
    Represents data parsed from a SEGMENTS file.
    """

    __tablename__ = "rsmdata_segments"

    content_hash = Column(VARBINARY(16), primary_key=True)
    line_number = Column(Integer, primary_key=True)
    group = Column(Integer)
    event_start = Column(E2UDE_DATETIME(3))
    event_stop = Column(E2UDE_DATETIME(3))
    flight_status = Column(String(100))
