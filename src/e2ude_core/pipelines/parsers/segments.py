from pathlib import Path
import pandas as pd
from e2ude_core.db.models import SegmentsData
import logging


def parse_segment(file_path: Path):
    try:
        with open(file_path) as f:
            lines = f.readlines()
    except Exception:
        logging.error("Could not read file during parsing", exc_info=True)
        raise

    COLUMNS = [
        "inc",
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
    keep_columns = [
        "line_number",
        "group",
        "event_start",
        "event_stop",
        "flight_status",
    ]

    rows = []
    for line in lines:
        tokens = line.split(",", maxsplit=len(COLUMNS))[:-1]
        rows.append(tokens)

    df = pd.DataFrame(rows, columns=COLUMNS)
    for date_col in ("event_start", "event_stop"):
        df[date_col] = pd.to_datetime(df[date_col], format="%m/%d/%Y %H:%M:%S:%f")
    df["line_number"] = df.index + 1
    return {SegmentsData: df[keep_columns]}
