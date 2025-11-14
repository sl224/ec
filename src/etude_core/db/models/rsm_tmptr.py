from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.dialects.mssql import DATETIME2
from etude_core.db.base_session import Base


class TmptrData(Base):
    """
    Represents data parsed from a TMPTR_LOG file.
    """

    __tablename__ = "tmptr"

    hash_id = Column(Integer, ForeignKey("file_hash_registry.id"), primary_key=True)
    line_number = Column(Integer, primary_key=True)

    datetime = Column(DateTime().with_variant(DATETIME2(3), "mssql"))
    category = Column(String)
    temp_f = Column(Integer)
    temp_c = Column(Integer)
