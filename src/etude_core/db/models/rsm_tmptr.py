from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.dialects.mssql import DATETIME2
from etude_core.db.models import Base, DEFAULT_SCHEMA


class TmptrData(Base):
    """
    Example RSM Zip derived table.
    Keyed by HashID
    """

    __tablename__ = "tmptr"

    # Primary Key is the HashID + Line Number
    hash_id = Column(
        Integer, ForeignKey(f"{DEFAULT_SCHEMA}.file_hash_registry.id"), primary_key=True
    )
    line_number = Column(Integer, primary_key=True)

    datetime = Column(DateTime().with_variant(DATETIME2(3), "mssql"))
    category = Column(String)
    temp_f = Column(Integer)
    temp_c = Column(Integer)
