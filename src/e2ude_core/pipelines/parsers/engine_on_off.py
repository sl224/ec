import pandas as pd
from pathlib import Path
from e2ude_core.db.models import EngineOnOff


def parse_engine_on_off_dataframe(file_path: Path) -> pd.DataFrame:
    columns = [
        "category",
        "engine_position",
        "start_time",
        "stop_time",
        "run_time",
        "src_file",
    ]
    df = pd.read_csv(
        file_path,
        names=columns,
        header=None,
        dtype=str,
        engine="python",
        on_bad_lines=lambda row: row[: len(columns)],
        skipinitialspace=True,
    )
    df["line_number"] = range(1, len(df) + 1)
    df = df[df["category"] == "ENG_TIME"].copy()
    df["start_time"] = pd.to_datetime(
        df["start_time"],
        format="%m/%d/%Y %H:%M:%S:%f",
        errors="coerce",
    )
    df["stop_time"] = pd.to_datetime(
        df["stop_time"],
        format="%m/%d/%Y %H:%M:%S:%f",
        errors="coerce",
    )
    run_time_parts = (
        df["run_time"]
        .astype("string")
        .str.strip()
        .str.extract(r"^(?P<hours>\d+):(?P<minutes>\d{2}):(?P<seconds>\d{2})$")
    )
    hours = pd.to_numeric(run_time_parts["hours"], errors="coerce")
    minutes = pd.to_numeric(run_time_parts["minutes"], errors="coerce")
    seconds = pd.to_numeric(run_time_parts["seconds"], errors="coerce")
    valid_duration = minutes.between(0, 59) & seconds.between(0, 59)
    df["run_time_seconds"] = (
        (hours * 3600 + minutes * 60 + seconds).where(valid_duration).astype("Int64")
    )

    return {EngineOnOff: df.drop(columns=["category", "src_file", "run_time"])}
