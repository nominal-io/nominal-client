from __future__ import annotations

import csv
import io
import logging
import struct
import sys

import numpy as np
import pytest

from nominal.thirdparty.ouster import _convert


def _write_buf3(path, field_names, records):
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


def test_parse_buf3_rejects_invalid_magic(tmp_path):
    path = tmp_path / "navdata2.daq"
    path.write_bytes(b"nope")

    with pytest.raises(ValueError, match="expected 'buf3' magic bytes"):
        _convert._parse_buf3(path)


def test_parse_buf3_reads_synthetic_records(tmp_path):
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
        {
            "_timestamp": 1_700_000_000.0,
            "navdata2tranrelx": 1.0,
            "navdata2tranrely": 2.0,
            "navdata2rpyrely": 0.25,
        },
        {
            "_timestamp": 1_700_000_001.0,
            "navdata2tranrelx": 3.0,
            "navdata2tranrely": 4.0,
            "navdata2rpyrely": 0.5,
        },
    ]


def test_parse_buf3_logs_and_stops_on_truncated_record(tmp_path, caplog):
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


def test_nav_trajectory_rejects_empty_records():
    with pytest.raises(ValueError, match="no valid poses"):
        _convert._NavTrajectory([])


def test_nav_trajectory_rejects_records_filtered_by_timestamp():
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


def test_nav_trajectory_interpolates_sorted_records():
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

    nav = _convert._NavTrajectory(records)
    trans, rpy = nav.interpolate(1_700_000_005.0)

    np.testing.assert_allclose(trans, np.array([5.0, 10.0, 15.0]))
    np.testing.assert_allclose(rpy, np.array([0.05, 0.1, 0.15]))
    np.testing.assert_allclose(nav.interpolate(1_699_999_999.0)[0], np.array([0.0, 0.0, 0.0]))
    np.testing.assert_allclose(nav.interpolate(1_700_000_011.0)[0], np.array([10.0, 20.0, 30.0]))


def test_rotation_matrix_yaw_rotates_x_axis_to_y_axis():
    result = _convert._rotation_matrix(0.0, 0.0, np.pi / 2) @ np.array([1.0, 0.0, 0.0])
    np.testing.assert_allclose(result, np.array([0.0, 1.0, 0.0]), atol=1e-12)


def test_frd_to_flu_translation_flips_y_and_z():
    np.testing.assert_allclose(_convert._translation_frd_to_flu(np.array([1.0, 2.0, 3.0])), np.array([1.0, -2.0, -3.0]))


def test_apply_transforms_applies_sensor_and_nav_offsets():
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


def test_parse_ouster_params_reads_sensor_offset(tmp_path):
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


def test_convert_ouster_dataset_validates_manifest_without_ouster_sdk(tmp_path, monkeypatch):
    monkeypatch.setattr(_convert, "_ensure_ouster_sdk", lambda: None)
    (tmp_path / "data.yaml").write_text("ousterDaqs: []\n")

    with pytest.raises(ValueError, match="No 'ousterDaqs' entries"):
        _convert.convert_ouster_dataset(tmp_path)


def test_convert_ouster_dataset_skips_missing_pcap(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(_convert, "_ensure_ouster_sdk", lambda: None)
    caplog.set_level(logging.INFO)
    (tmp_path / "data.yaml").write_text("ousterDaqs:\n  - pcap: missing.pcap\n    info: missing.json\n")

    assert _convert.convert_ouster_dataset(tmp_path, apply_nav=False) == []
    assert "Skipping missing.pcap" in caplog.text
    assert "Ouster conversion skipped 1/1 sensor(s)" in caplog.text
    assert "Ouster conversion produced 0/1 CSV file(s)" in caplog.text


def test_convert_ouster_dataset_fail_fast_on_missing_pcap(tmp_path, monkeypatch):
    monkeypatch.setattr(_convert, "_ensure_ouster_sdk", lambda: None)
    (tmp_path / "data.yaml").write_text("ousterDaqs:\n  - pcap: missing.pcap\n    info: missing.json\n")

    with pytest.raises(FileNotFoundError, match="missing.pcap"):
        _convert.convert_ouster_dataset(tmp_path, apply_nav=False, fail_on_missing_files=True)


def test_convert_ouster_dataset_fail_fast_on_missing_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(_convert, "_ensure_ouster_sdk", lambda: None)
    (tmp_path / "data.yaml").write_text("ousterDaqs:\n  - pcap: present.pcap\n    info: missing.json\n")
    (tmp_path / "present.pcap").write_bytes(b"")

    with pytest.raises(FileNotFoundError, match="missing.json"):
        _convert.convert_ouster_dataset(tmp_path, apply_nav=False, fail_on_missing_files=True)


def test_convert_ouster_dataset_rejects_invalid_point_threshold(tmp_path, monkeypatch):
    monkeypatch.setattr(_convert, "_ensure_ouster_sdk", lambda: None)

    with pytest.raises(ValueError, match="min_valid_point_distance_m"):
        _convert.convert_ouster_dataset(tmp_path, min_valid_point_distance_m=-0.1)


def test_convert_ouster_dataset_rejects_invalid_progress_interval(tmp_path, monkeypatch):
    monkeypatch.setattr(_convert, "_ensure_ouster_sdk", lambda: None)

    with pytest.raises(ValueError, match="progress_log_interval_scans"):
        _convert.convert_ouster_dataset(tmp_path, progress_log_interval_scans=0)


def test_valid_point_mask_uses_configured_distance_threshold():
    pts = np.array(
        [
            [0.0, 0.0, 0.0],
            [0.05, 0.0, 0.0],
            [0.2, 0.0, 0.0],
        ]
    )

    np.testing.assert_array_equal(_convert._valid_point_mask(pts, 0.1), np.array([False, False, True]))
    np.testing.assert_array_equal(_convert._valid_point_mask(pts, 0.0), np.array([False, True, True]))


def test_should_log_progress_uses_configured_interval():
    assert _convert._should_log_progress(100, 100)
    assert not _convert._should_log_progress(50, 100)
    assert not _convert._should_log_progress(100, None)


def test_write_scan_points_formats_rows():
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


def test_ensure_ouster_sdk_reports_optional_dependency(monkeypatch):
    monkeypatch.setitem(sys.modules, "ouster", None)

    with pytest.raises(ImportError, match="pip install nominal\\[ouster\\] ouster-sdk"):
        _convert._ensure_ouster_sdk()
