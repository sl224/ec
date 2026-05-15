import pandas as pd

from e2ude_core.db.models import EngineOnOff, TmptrData
from e2ude_core.pipelines.parsers.engine_on_off import parse_engine_on_off_dataframe
from e2ude_core.pipelines.parsers.tmptr import parse_tmptr_dataframe


def test_parse_tmptr_extracts_timestamps_and_temperatures(tmp_path):
    tmptr_file = tmp_path / "TMPTR_LOG"
    tmptr_file.write_text(
        "\n".join(
            [
                "AFMC,00250203,01:09:02.123,TMPTR,029C,085F",
                "AFMC,00250203,01:10:02.123,TMPTR,030C,bad",
            ]
        ),
        encoding="utf-8",
    )

    df = parse_tmptr_dataframe(tmptr_file)[TmptrData]

    assert list(df.columns) == [
        "afmc",
        "category",
        "temp_f",
        "temp_c",
        "datetime",
        "line_number",
    ]
    assert df["line_number"].tolist() == [1, 2]
    assert df["datetime"].tolist() == [
        pd.Timestamp("2025-02-03 01:09:02.123"),
        pd.Timestamp("2025-02-03 01:10:02.123"),
    ]
    assert df["temp_f"].iloc[0] == 85
    assert pd.isna(df["temp_f"].iloc[1])
    assert df["temp_c"].tolist() == [29, 30]


def test_parse_tmptr_uses_temperature_unit_suffix_not_position(tmp_path):
    tmptr_file = tmp_path / "TMPTR_LOG"
    tmptr_file.write_text(
        "\n".join(
            [
                "AFMC,00250203,01:09:02.123,TMPTR,029C,085F",
                "AFMC,00250203,01:10:02.123,TMPTR,086F,030C",
            ]
        ),
        encoding="utf-8",
    )

    df = parse_tmptr_dataframe(tmptr_file)[TmptrData]

    assert df["temp_f"].tolist() == [85, 86]
    assert df["temp_c"].tolist() == [29, 30]


def test_parse_tmptr_tolerates_extra_and_short_rows(tmp_path):
    tmptr_file = tmp_path / "TMPTR_LOG"
    tmptr_file.write_text(
        "\n".join(
            [
                "AFMC,00250203,01:09:02.123,TMPTR,029C,085F",
                "too,short",
                "AFMC,00250203,01:10:02.123,TMPTR,030C,086F,ignored,ignored",
            ]
        ),
        encoding="utf-8",
    )

    df = parse_tmptr_dataframe(tmptr_file)[TmptrData]

    assert df["line_number"].tolist() == [1, 3]
    assert df["temp_f"].tolist() == [85, 86]
    assert df["temp_c"].tolist() == [29, 30]


def test_parse_tmptr_skips_non_data_rows_without_failing(tmp_path):
    tmptr_file = tmp_path / "TMPTR_LOG"
    tmptr_file.write_text(
        "\n".join(
            [
                "AFMC,00250203,01:09:02.123,TMPTR,029C,085F",
                "AFMC1,20010101,AFMC1,TMPTR,030C,086F",
                "20010101,AFMC1,not,time,TMPTR,030C,086F,extra,extra,extra,extra",
            ]
        ),
        encoding="utf-8",
    )

    df = parse_tmptr_dataframe(tmptr_file)[TmptrData]

    assert df["line_number"].tolist() == [1]
    assert df.iloc[0]["datetime"] == pd.Timestamp("2025-02-03 01:09:02.123")


def test_parse_engine_on_off_filters_engine_time_rows(tmp_path):
    engine_file = tmp_path / "sample_Engine"
    engine_file.write_text(
        "\n".join(
            [
                "OTHER,L,02/03/2025 01:00:00:000,02/03/2025 01:05:00:000,00:05:00,src",
                "ENG_TIME,R,02/03/2025 01:09:02:000,02/03/2025 01:19:02:000,00:10:00,src",
            ]
        ),
        encoding="utf-8",
    )

    df = parse_engine_on_off_dataframe(engine_file)[EngineOnOff]

    assert list(df.columns) == [
        "engine_position",
        "start_time",
        "stop_time",
        "line_number",
        "run_time_seconds",
    ]
    assert df["line_number"].tolist() == [2]
    assert df.iloc[0]["engine_position"] == "R"
    assert df.iloc[0]["start_time"] == pd.Timestamp("2025-02-03 01:09:02")
    assert df.iloc[0]["stop_time"] == pd.Timestamp("2025-02-03 01:19:02")
    assert df.iloc[0]["run_time_seconds"] == 600


def test_parse_engine_on_off_tolerates_extra_fields_without_multiindex(tmp_path):
    engine_file = tmp_path / "sample_Engine"
    engine_file.write_text(
        "\n".join(
            [
                "ENG_EFF",
                "ENG_TIME,R,02/03/2025 01:09:02:000,02/03/2025 01:19:02:000,00:10:00,src,ignored",
            ]
        ),
        encoding="utf-8",
    )

    df = parse_engine_on_off_dataframe(engine_file)[EngineOnOff]

    assert df["line_number"].tolist() == [2]
    assert df.iloc[0]["engine_position"] == "R"
    assert df.iloc[0]["run_time_seconds"] == 600


def test_parse_engine_on_off_rejects_invalid_duration_parts(tmp_path):
    engine_file = tmp_path / "sample_Engine"
    engine_file.write_text(
        "ENG_TIME,R,02/03/2025 01:09:02:000,02/03/2025 01:19:02:000,00:99:00,src",
        encoding="utf-8",
    )

    df = parse_engine_on_off_dataframe(engine_file)[EngineOnOff]

    assert pd.isna(df.iloc[0]["run_time_seconds"])
