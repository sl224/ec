# %%
from pathlib import Path
import pandas as pd

# TODO setup a network drive location for test assets
STATIC_ASSETS_ROOT = Path("tests/static_assets")

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


def get_segment_df(lines):

    COLUMNS = [
        "line_number",
        "group",
        "flight",
        "event_start",
        "event_stop",
        None,
        None,
        "ground_time",
        "flight_time",
        "landing",
        "catapults",
        "arrests",
        "flight_status",
    ]
    keep_columns = ["line_number", "group", "event_start", "event_stop", "flight_status"]

    rows = []
    for line in lines:
        tokens = line.split(",", maxsplit=len(COLUMNS))[:-1]
        rows.append(tokens)

    df = pd.DataFrame(rows, columns=COLUMNS)
    for date_col in ("event_start", "event_stop"):
        df[date_col] = pd.to_datetime(df[date_col], format="%m/%d/%Y %H:%M:%S:%f")

    return {SegmentsData: df[keep_columns]}





def test_parse_segment():
    test_strs = [
        "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
        "2,1,,01/13/2025 15:36:51:825,01/13/2025 18:38:47:337,01/13/2025 15:36:36:825,01/13/2025 18:43:47:337,,03:07:10,true,false,false,Flight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
        "3,2,,01/13/2025 18:38:47:337,01/13/2025 19:08:32:337,01/13/2025 18:43:47:337,01/13/2025 19:08:17:337,00:24:30,,false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
        "4,2,,01/13/2025 19:08:32:337,01/13/2025 22:07:57:337,01/13/2025 19:08:17:337,01/13/2025 22:12:57:337,,03:04:40,true,false,false,Flight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
        "5,3,,01/13/2025 22:07:57:337,01/13/2025 22:44:57:825,01/13/2025 22:12:57:337,01/13/2025 22:44:42:825,00:31:45,,false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
        "6,3,,01/13/2025 22:44:57:825,01/14/2025 01:22:15:325,01/13/2025 22:44:42:825,01/14/2025 01:27:15:325,,02:42:32,true,false,false,Flight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
        "7,3,,01/14/2025 01:22:15:325,01/14/2025 01:33:58:075,01/14/2025 01:27:15:325,01/14/2025 01:33:58:075,00:06:42,,false,false,false,PostFlight,1,false,false,1690830113251412_MAINT_00,,false,0,0,,,",
    ]

    df = get_segment_df(test_strs)
    print(df)


if __name__ == "__main__":
    test_parse_segment()

# %%
