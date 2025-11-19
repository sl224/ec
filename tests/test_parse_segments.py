# %%
from pathlib import Path
import pandas as pd
import sqlalchemy as sa

# TODO setup a network drive location for test assets
STATIC_ASSETS_ROOT = Path("tests/static_assets")


def get_segment_df(lines):
    COLUMNS = [
        "LineNumber",
        "Group",
        "Flight",
        "Event Start",
        "Event Stop",
        None,
        None,
        "Ground Time",
        "Flight Time",
        "Landing",
        "Catapults",
        "Arrests",
        "Remarks",
    ]

    rows = []
    for line in lines:
        tokens = line.split(",", maxsplit=len(COLUMNS))[:-1]
        rows.append(tokens)

    df = pd.DataFrame(rows, columns=COLUMNS)
    for date_col in ("Event Start", "Event Stop"):
        df[date_col] = pd.to_datetime(df[date_col], format="%m/%d/%Y %H:%M:%S:%f")

    return df


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
