from e2ude_core.db.models import SegmentsData
from e2ude_core.pipelines.parsers.segments import parse_segment


def test_parse_segment_extracts_expected_columns(tmp_path):
    segment_file = tmp_path / "sample_Segments"
    segment_file.write_text(
        "\n".join(
            [
                (
                    "1,1,,01/13/2025 14:13:36:825,01/13/2025 15:36:51:825,"
                    "01/13/2025 14:13:36:825,01/13/2025 15:36:36:825,01:23:00,,"
                    "false,false,false,PreFlight,1,false,false,1690830113251412_MAINT_00,,"
                    "false,0,0,,,"
                ),
                (
                    "2,1,,01/13/2025 15:36:51:825,01/13/2025 18:38:47:337,"
                    "01/13/2025 15:36:36:825,01/13/2025 18:43:47:337,,03:07:10,"
                    "true,false,false,Flight,1,false,false,1690830113251412_MAINT_00,,"
                    "false,0,0,,,"
                ),
            ]
        ),
        encoding="utf-8",
    )

    payload = parse_segment(segment_file)
    df = payload[SegmentsData]

    assert list(df.columns) == [
        "line_number",
        "group",
        "event_start",
        "event_stop",
        "flight_status",
    ]
    assert df["line_number"].tolist() == [1, 2]
    assert df["group"].tolist() == ["1", "1"]
    assert df["flight_status"].tolist() == ["PreFlight", "Flight"]
