from __future__ import annotations

import csv
import io
import logging
import struct
from collections.abc import Callable, Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from nominal.ouster import _convert


def _write_buf3(
    path: Path,
    field_names: list[str],
    records: list[tuple[float, dict[int, float]]],
) -> None:
    """Encode a minimal buf3 nav file from field names and (timestamp, {field_index: value}) records."""
    fields_raw = " ".join(field_names).encode("ascii")
    data = bytearray(b"buf3")
    data.extend(struct.pack("<I", 0))
    data.extend(b"\0")
    data.extend(struct.pack("<I", 4))
    data.extend(b"date")
    data.extend(struct.pack("<I", len(fields_raw)))
    data.extend(fields_raw)

    for timestamp, values in records:
        data.extend(struct.pack("<d", timestamp))
        data.extend(struct.pack("<I", len(values) + 1))
        data.extend(struct.pack(f"<{len(values)}H", *values.keys()))
        data.extend(struct.pack(f"<{len(values)}d", *values.values()))

    path.write_bytes(data)


@pytest.fixture
def stub_ouster_sdk() -> Iterator[MagicMock]:
    """Stub the ouster-sdk import check so dataset conversion can run without decoding a real PCAP."""
    with patch.object(_convert, "_ensure_ouster_sdk") as stub:
        yield stub


@pytest.fixture
def write_data_yaml(tmp_path: Path) -> Callable[[str], Path]:
    """Factory that writes data.yaml into a fresh dataset directory and returns that directory."""

    def _write(contents: str) -> Path:
        (tmp_path / "data.yaml").write_text(contents)
        return tmp_path

    return _write


def test_parse_buf3_rejects_invalid_magic(tmp_path: Path) -> None:
    """Rejects a nav file that does not start with the buf3 magic bytes."""
    path = tmp_path / "navdata2.daq"
    path.write_bytes(b"nope")

    with pytest.raises(ValueError, match="expected 'buf3' magic bytes"):
        _convert._parse_buf3(path)


def test_parse_buf3_reads_records(tmp_path: Path) -> None:
    """Reads each record's timestamp and field values back out of a buf3 file."""
    path = tmp_path / "navdata2.daq"
    field_names = ["navdata2tranrelx", "navdata2tranrely", "navdata2rpyrely"]
    _write_buf3(
        path,
        field_names,
        [
            (1_700_000_000.0, {1: 1.0, 2: 2.0, 3: 0.25}),
            (1_700_000_001.0, {1: 3.0, 2: 4.0, 3: 0.5}),
        ],
    )

    parsed_fields, records = _convert._parse_buf3(path)

    assert parsed_fields == field_names
    assert records == [
        {"_timestamp": 1_700_000_000.0, "navdata2tranrelx": 1.0, "navdata2tranrely": 2.0, "navdata2rpyrely": 0.25},
        {"_timestamp": 1_700_000_001.0, "navdata2tranrelx": 3.0, "navdata2tranrely": 4.0, "navdata2rpyrely": 0.5},
    ]


def test_parse_buf3_stops_and_logs_on_truncated_record(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Stops parsing and logs a warning when a trailing record is truncated, keeping the records read so far."""
    path = tmp_path / "navdata2.daq"
    field_names = ["navdata2tranrelx"]
    _write_buf3(path, field_names, [(1_700_000_000.0, {1: 1.0})])
    data = bytearray(path.read_bytes())
    data.extend(struct.pack("<d", 1_700_000_001.0))
    data.extend(struct.pack("<I", 10))
    data.extend(b"\0")
    path.write_bytes(data)

    parsed_fields, records = _convert._parse_buf3(path)

    assert parsed_fields == field_names
    assert records == [{"_timestamp": 1_700_000_000.0, "navdata2tranrelx": 1.0}]
    assert "Invalid buf3 record" in caplog.text


def test_nav_trajectory_rejects_empty_records() -> None:
    """Raises when there are no records to build a trajectory from."""
    with pytest.raises(ValueError, match="no valid poses"):
        _convert._NavTrajectory([])


def test_nav_trajectory_rejects_records_below_timestamp_floor() -> None:
    """Raises when every record's timestamp is below the validity floor."""
    records = [
        {
            "_timestamp": _convert.MIN_VALID_NAV_TIMESTAMP - 1,
            "navdata2tranrelx": 1.0,
            "navdata2tranrely": 2.0,
            "navdata2tranrelz": 3.0,
        }
    ]

    with pytest.raises(ValueError, match="no valid poses"):
        _convert._NavTrajectory(records)


def test_nav_trajectory_interpolates_between_poses() -> None:
    """Linearly interpolates translation and rpy between the two surrounding poses (sorting by time first)."""
    records = [
        {
            "_timestamp": 1_700_000_010.0,
            "navdata2tranrelx": 10.0,
            "navdata2tranrely": 20.0,
            "navdata2tranrelz": 30.0,
            "navdata2rpyrelr": 0.1,
            "navdata2rpyrelp": 0.2,
            "navdata2rpyrely": 0.3,
        },
        {
            "_timestamp": 1_700_000_000.0,
            "navdata2tranrelx": 0.0,
            "navdata2tranrely": 0.0,
            "navdata2tranrelz": 0.0,
            "navdata2rpyrelr": 0.0,
            "navdata2rpyrelp": 0.0,
            "navdata2rpyrely": 0.0,
        },
    ]

    trans, rpy = _convert._NavTrajectory(records).interpolate(1_700_000_005.0)

    np.testing.assert_allclose(trans, np.array([5.0, 10.0, 15.0]))
    np.testing.assert_allclose(rpy, np.array([0.05, 0.1, 0.15]))


def test_nav_trajectory_clamps_outside_time_range() -> None:
    """Clamps to the first/last pose for query times before or after the trajectory's range."""
    records = [
        {"_timestamp": 1_700_000_000.0, "navdata2tranrelx": 0.0, "navdata2tranrely": 0.0, "navdata2tranrelz": 0.0},
        {"_timestamp": 1_700_000_010.0, "navdata2tranrelx": 10.0, "navdata2tranrely": 20.0, "navdata2tranrelz": 30.0},
    ]
    nav = _convert._NavTrajectory(records)

    np.testing.assert_allclose(nav.interpolate(1_699_999_999.0)[0], np.array([0.0, 0.0, 0.0]))
    np.testing.assert_allclose(nav.interpolate(1_700_000_011.0)[0], np.array([10.0, 20.0, 30.0]))


def test_rotation_matrix_yaw_rotates_x_axis_to_y_axis() -> None:
    """Rotates the x-axis onto the y-axis for a 90° yaw."""
    result = _convert._rotation_matrix(0.0, 0.0, np.pi / 2) @ np.array([1.0, 0.0, 0.0])
    np.testing.assert_allclose(result, np.array([0.0, 1.0, 0.0]), atol=1e-12)


def test_frd_to_flu_translation_flips_y_and_z() -> None:
    """Converts an FRD translation to FLU by negating the y and z components."""
    np.testing.assert_allclose(_convert._translation_frd_to_flu(np.array([1.0, 2.0, 3.0])), np.array([1.0, -2.0, -3.0]))


def test_apply_transforms_composes_sensor_and_nav_offsets() -> None:
    """Applies the sensor mount offset and then the nav pose to a point, in FLU coordinates."""
    pts = np.array([[1.0, 2.0, 3.0]])
    sensor_offset = (np.array([10.0, 20.0, 30.0]), np.array([0.0, 0.0, 0.0]))
    nav = _convert._NavTrajectory(
        [
            {
                "_timestamp": 1_700_000_000.0,
                "navdata2tranrelx": 100.0,
                "navdata2tranrely": 200.0,
                "navdata2tranrelz": 300.0,
            }
        ]
    )

    transformed = _convert._apply_transforms(pts, sensor_offset, nav, 1_700_000_000.0)

    np.testing.assert_allclose(transformed, np.array([[111.0, -218.0, -327.0]]))


def test_parse_ouster_params_reads_sensor_offset(tmp_path: Path) -> None:
    """Parses sensor translation and rpy from an ouster params file, ignoring comments and junk."""
    path = tmp_path / "ouster.params"
    path.write_text(
        "\n".join(
            [
                "# comment",
                "sensorX = 1.5",
                "sensorY = -2.0",
                "sensorZ = 3.25",
                "sensorRoll = 0.1",
                "sensorPitch = 0.2",
                "sensorYaw = 0.3",
                "ignored = nope",
            ]
        )
    )

    result = _convert._parse_ouster_params(path)

    assert result is not None
    trans, rpy = result
    np.testing.assert_allclose(trans, np.array([1.5, -2.0, 3.25]))
    np.testing.assert_allclose(rpy, np.array([0.1, 0.2, 0.3]))


def test_convert_ouster_dataset_rejects_manifest_without_daqs(
    stub_ouster_sdk: MagicMock, write_data_yaml: Callable[[str], Path]
) -> None:
    """Raises when the manifest contains no ousterDaqs entries."""
    dataset_dir = write_data_yaml("ousterDaqs: []\n")
    with pytest.raises(ValueError, match="No 'ousterDaqs' entries"):
        _convert.convert_ouster_dataset(dataset_dir)


def test_convert_ouster_dataset_skips_missing_pcap(
    stub_ouster_sdk: MagicMock,
    write_data_yaml: Callable[[str], Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Skips (rather than fails) a sensor whose PCAP is missing and logs the skipped/produced summary."""
    caplog.set_level(logging.INFO)
    dataset_dir = write_data_yaml("ousterDaqs:\n  - pcap: missing.pcap\n    info: missing.json\n")

    assert _convert.convert_ouster_dataset(dataset_dir, apply_nav=False) == []
    assert "Skipping missing.pcap" in caplog.text
    assert "Ouster conversion skipped 1/1 sensor(s)" in caplog.text
    assert "Ouster conversion produced 0/1 CSV file(s)" in caplog.text


def test_convert_ouster_dataset_fails_fast_on_missing_pcap(
    stub_ouster_sdk: MagicMock, write_data_yaml: Callable[[str], Path]
) -> None:
    """Raises on a missing PCAP when fail_on_missing_files is set."""
    dataset_dir = write_data_yaml("ousterDaqs:\n  - pcap: missing.pcap\n    info: missing.json\n")
    with pytest.raises(FileNotFoundError, match="missing.pcap"):
        _convert.convert_ouster_dataset(dataset_dir, apply_nav=False, fail_on_missing_files=True)


def test_convert_ouster_dataset_fails_fast_on_missing_metadata(
    stub_ouster_sdk: MagicMock, write_data_yaml: Callable[[str], Path]
) -> None:
    """Raises on missing sensor metadata when fail_on_missing_files is set, even if the PCAP exists."""
    dataset_dir = write_data_yaml("ousterDaqs:\n  - pcap: present.pcap\n    info: missing.json\n")
    (dataset_dir / "present.pcap").write_bytes(b"")
    with pytest.raises(FileNotFoundError, match="missing.json"):
        _convert.convert_ouster_dataset(dataset_dir, apply_nav=False, fail_on_missing_files=True)


def test_convert_ouster_dataset_rejects_negative_point_threshold(stub_ouster_sdk: MagicMock, tmp_path: Path) -> None:
    """Raises when min_valid_point_distance_m is negative."""
    with pytest.raises(ValueError, match="min_valid_point_distance_m"):
        _convert.convert_ouster_dataset(tmp_path, min_valid_point_distance_m=-0.1)


def test_convert_ouster_dataset_rejects_non_positive_progress_interval(
    stub_ouster_sdk: MagicMock, tmp_path: Path
) -> None:
    """Raises when progress_log_interval_scans is not positive."""
    with pytest.raises(ValueError, match="progress_log_interval_scans"):
        _convert.convert_ouster_dataset(tmp_path, progress_log_interval_scans=0)


@pytest.mark.parametrize(
    "threshold, expected",
    [
        (0.1, [False, False, True]),
        (0.0, [False, True, True]),
    ],
)
def test_valid_point_mask_filters_by_distance(threshold: float, expected: list[bool]) -> None:
    """Keeps only points whose distance from the origin exceeds the configured threshold."""
    pts = np.array([[0.0, 0.0, 0.0], [0.05, 0.0, 0.0], [0.2, 0.0, 0.0]])
    np.testing.assert_array_equal(_convert._valid_point_mask(pts, threshold), np.array(expected))


@pytest.mark.parametrize(
    "scan_count, interval, expected",
    [
        (100, 100, True),
        (50, 100, False),
        (100, None, False),
    ],
)
def test_should_log_progress(scan_count: int, interval: int | None, expected: bool) -> None:
    """Logs progress only on multiples of the interval, and never when the interval is disabled."""
    assert _convert._should_log_progress(scan_count, interval) is expected


def test_write_scan_points_formats_rows() -> None:
    """Writes one CSV row per point with fixed-precision coordinates and integer-rounded channels."""
    output = io.StringIO()
    writer = csv.writer(output)

    _convert._write_scan_points(
        writer,
        np.array([[1.23456, 2.34567, 3.45678]]),
        np.array([42.4]),
        np.array([17.6]),
        np.array([9.2]),
        0.1234567,
    )

    assert output.getvalue() == "1.2346,2.3457,3.4568,0.123457,42,18,9\r\n"


def test_ensure_ouster_sdk_reports_optional_dependency() -> None:
    """Raises a helpful ImportError pointing at the [ouster] extra when the SDK cannot be imported."""
    with (
        patch("importlib.import_module", side_effect=ImportError("no module named ouster")),
        pytest.raises(ImportError, match=r"pip install nominal\[ouster\] ouster-sdk"),
    ):
        _convert._ensure_ouster_sdk()
