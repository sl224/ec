from sqlalchemy import Column, Integer, String, VARBINARY

from e2ude_core.db.base_session import Base, E2UDE_DATETIME


class TmptrData(Base):
    """
    Represents data parsed from a TMPTR_LOG file.
    """

    __tablename__ = "rsmdata_tmptr"

    content_hash = Column(VARBINARY(16), primary_key=True)
    line_number = Column(Integer, primary_key=True)
    afmc = Column(String(10))
    datetime = Column(E2UDE_DATETIME(3))
    category = Column(String)
    temp_f = Column(Integer)
    temp_c = Column(Integer)
