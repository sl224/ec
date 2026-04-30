import pandas as pd
from pathlib import Path
from e2ude_core.db.models import EngineOnOff


def parse_engine_on_off_dataframe(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        names=[
            "category",
            "engine_position",
            "start_time",
            "stop_time",
            "run_time",
            "src_file",
        ],
        header=None,
    )
    df["line_number"] = df.index + 1
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
    df["run_time"] = pd.to_datetime(
        df["run_time"],
        format="%H:%M:%S",
        errors="coerce",
    )

    return {EngineOnOff: df.drop(columns=["category", "src_file"])}
