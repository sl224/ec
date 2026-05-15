import csv
import pandas as pd
from pathlib import Path
from e2ude_core.db.models import TmptrData


def _assign_temperature_columns(df: pd.DataFrame) -> None:
    df["temp_f"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    df["temp_c"] = pd.Series(pd.NA, index=df.index, dtype="Int64")

    for raw_col in ("temp_1_raw", "temp_2_raw"):
        raw = df[raw_col].astype("string").str.strip()
        units = raw.str[-1].str.upper()
        values = pd.to_numeric(raw.str[:-1], errors="coerce").astype("Int64")

        fahrenheit = units.eq("F").fillna(False)
        celsius = units.eq("C").fillna(False)
        df.loc[fahrenheit, "temp_f"] = values[fahrenheit]
        df.loc[celsius, "temp_c"] = values[celsius]


def parse_tmptr_dataframe(file_path: Path) -> pd.DataFrame:
    """
    Reads a TMPTR_LOG file and returns a cleaned DataFrame.
    """
    columns = ["afmc", "date", "time", "category", "temp_1_raw", "temp_2_raw"]
    rows = []
    with file_path.open(newline="", encoding="utf-8", errors="replace") as handle:
        for line_number, row in enumerate(csv.reader(handle, skipinitialspace=True), 1):
            if len(row) < len(columns):
                continue
            record = dict(zip(columns, row[: len(columns)]))
            record["line_number"] = line_number
            rows.append(record)

    df = pd.DataFrame(rows, columns=[*columns, "line_number"])
    if df.empty:
        return {
            TmptrData: pd.DataFrame(
                columns=[
                    "afmc",
                    "category",
                    "temp_f",
                    "temp_c",
                    "datetime",
                    "line_number",
                ]
            )
        }

    df = df.dropna(subset=["date", "time", "temp_1_raw", "temp_2_raw"]).copy()

    _assign_temperature_columns(df)

    date_str = df["date"].str.strip()
    time_str = df["time"].str.strip()
    valid_datetime_shape = date_str.str.fullmatch(r"\d{8}") & time_str.str.fullmatch(
        r"\d{2}:\d{2}:\d{2}\.\d+"
    )
    df = df[valid_datetime_shape.fillna(False)].copy()
    date_str = date_str[valid_datetime_shape.fillna(False)]
    time_str = time_str[valid_datetime_shape.fillna(False)]

    # Fix '00' year prefix
    date_str_fixed = date_str.str.replace(r"^00", "20", regex=True)
    datetime_full = date_str_fixed + " " + time_str

    # Convert to datetime
    df["datetime"] = pd.to_datetime(
        datetime_full, format="%Y%m%d %H:%M:%S.%f", errors="coerce"
    )
    df = df.dropna(subset=["datetime"]).copy()

    return {
        TmptrData: df[
            ["afmc", "category", "temp_f", "temp_c", "datetime", "line_number"]
        ]
    }
