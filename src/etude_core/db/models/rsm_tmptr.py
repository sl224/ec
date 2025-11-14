from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.dialects.mssql import DATETIME2

# Import Base AND the new schema_fkey helper
from etude_core.db.base_session import Base, schema_fkey


class TmptrData(Base):
    """
    Represents data parsed from a TMPTR_LOG file.
    """

    __tablename__ = "rsmdata_tmptr"

    # Use `schema_fkey` to create a schema-qualified foreign key reference.
    hash_id = Column(
        Integer, ForeignKey(schema_fkey("metadata_hash_registry.id")), primary_key=True
    )
    line_number = Column(Integer, primary_key=True)

    datetime = Column(DateTime().with_variant(DATETIME2(3), "mssql"))
    category = Column(String)
    temp_f = Column(Integer)
    temp_c = Column(Integer)
