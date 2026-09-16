from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import pytest

from nominal.thirdparty.ouster._convert import (
    _find_metadata,
    _find_nav_file,
    _NavTrajectory,
    _parse_buf3,
    _parse_ouster_params,
    _rotation_frd_to_flu,
    _rotation_matrix,
    convert_ouster_dataset,
)


def _buf3_bytes(field_names: list[str], records: list[tuple[float, dict[int, float]]]) -> bytes:
    """Build a minimal buf3 payload: header, date string, field list, then timestamped records."""
    date = b"2026-09-14"
    out = bytearray(b"buf3" + b"\x00" * 4 + b"\x00")
    out += struct.pack("<I", len(date)) + date

    flist = (" ".join(field_names) + " ").encode("ascii")
    out += struct.pack("<I", len(flist)) + flist

    for ts, values in records:
        out += struct.pack("<d", ts)
        # The count is one greater than the number of (index, value) pairs that follow.
        out += struct.pack("<I", len(values) + 1)
        for idx in values:
            out += struct.pack("<H", idx)
        for val in values.values():
            out += struct.pack("<d", val)
    return bytes(out)


class TestParseBuf3:
    def test_reads_field_names_and_records(self, tmp_path: Path) -> None:
        path = tmp_path / "navdata2.daq"
        # Field indices are 1-based in the file, so index 1 is "alpha" and 2 is "beta".
        path.write_bytes(_buf3_bytes(["alpha", "beta"], [(1e9, {1: 1.5, 2: 2.5}), (2e9, {1: 3.5})]))

        field_names, records = _parse_buf3(path)

        assert field_names == ["alpha", "beta"]
        assert records == [
            {"_timestamp": 1e9, "alpha": 1.5, "beta": 2.5},
            {"_timestamp": 2e9, "alpha": 3.5},
        ]

    def test_rejects_a_file_with_the_wrong_magic(self, tmp_path: Path) -> None:
        path = tmp_path / "navdata2.daq"
        path.write_bytes(b"nope" + b"\x00" * 32)

        with pytest.raises(ValueError, match="Not a buf3 file"):
            _parse_buf3(path)

    def test_ignores_indices_outside_the_field_list(self, tmp_path: Path) -> None:
        path = tmp_path / "navdata2.daq"
        path.write_bytes(_buf3_bytes(["alpha"], [(1e9, {1: 1.5, 9: 9.9})]))

        _, records = _parse_buf3(path)

        assert records == [{"_timestamp": 1e9, "alpha": 1.5}]


class TestNavTrajectory:
    @staticmethod
    def _record(ts: float, x: float, yaw: float) -> dict[str, float]:
        return {"_timestamp": ts, "navdata2tranrelx": x, "navdata2rpyrely": yaw}

    def test_interpolates_between_the_bracketing_poses(self) -> None:
        nav = _NavTrajectory([self._record(1e9, 0.0, 0.0), self._record(2e9, 10.0, 1.0)])

        trans, rpy = nav.interpolate(1.5e9)

        assert trans[0] == pytest.approx(5.0)
        assert rpy[2] == pytest.approx(0.5)

    def test_clamps_outside_the_pose_range(self) -> None:
        nav = _NavTrajectory([self._record(1e9, 0.0, 0.0), self._record(2e9, 10.0, 1.0)])

        assert nav.interpolate(0.0)[0][0] == pytest.approx(0.0)
        assert nav.interpolate(9e9)[0][0] == pytest.approx(10.0)

    def test_sorts_poses_that_arrive_out_of_order(self) -> None:
        nav = _NavTrajectory([self._record(2e9, 10.0, 1.0), self._record(1e9, 0.0, 0.0)])

        assert nav.interpolate(1.5e9)[0][0] == pytest.approx(5.0)

    def test_drops_records_with_pre_epoch_timestamps(self) -> None:
        # Timestamps below 1e9 are placeholders rather than real GMT readings.
        nav = _NavTrajectory([self._record(0.0, 99.0, 0.0), self._record(1e9, 1.0, 0.0), self._record(2e9, 2.0, 0.0)])

        assert nav.interpolate(1e9)[0][0] == pytest.approx(1.0)

    def test_rejects_a_trajectory_with_no_usable_poses(self) -> None:
        with pytest.raises(ValueError, match="apply_nav=False"):
            _NavTrajectory([self._record(0.0, 1.0, 0.0)])


class TestRotations:
    def test_identity_for_zero_angles(self) -> None:
        np.testing.assert_allclose(_rotation_matrix(0.0, 0.0, 0.0), np.eye(3), atol=1e-12)

    def test_yaw_rotates_x_towards_y(self) -> None:
        rotated = _rotation_matrix(0.0, 0.0, np.pi / 2) @ np.array([1.0, 0.0, 0.0])
        np.testing.assert_allclose(rotated, [0.0, 1.0, 0.0], atol=1e-12)

    def test_frd_to_flu_flips_the_yaw_direction(self) -> None:
        # Conjugating by diag(1, -1, -1) turns a right-handed-down yaw into a left-handed-up one.
        rotated = _rotation_frd_to_flu(0.0, 0.0, np.pi / 2) @ np.array([1.0, 0.0, 0.0])
        np.testing.assert_allclose(rotated, [0.0, -1.0, 0.0], atol=1e-12)

    def test_frd_to_flu_is_its_own_inverse_for_zero_angles(self) -> None:
        np.testing.assert_allclose(_rotation_frd_to_flu(0.0, 0.0, 0.0), np.eye(3), atol=1e-12)


class TestParseOusterParams:
    def test_reads_translation_and_orientation(self, tmp_path: Path) -> None:
        path = tmp_path / "ouster.cfg"
        path.write_text(
            "# a comment\n"
            "sensorX = 1.0\n"
            "sensorY=2.0\n"
            "sensorZ = 3.0\n"
            "sensorRoll = 0.1\n"
            "sensorPitch = 0.2\n"
            "sensorYaw = 0.3\n"
            "junk line with no equals\n"
            "notANumber = abc\n"
        )

        parsed = _parse_ouster_params(path)

        assert parsed is not None
        trans, rpy = parsed
        np.testing.assert_allclose(trans, [1.0, 2.0, 3.0])
        np.testing.assert_allclose(rpy, [0.1, 0.2, 0.3])

    def test_returns_none_without_a_sensor_offset(self, tmp_path: Path) -> None:
        path = tmp_path / "ouster.cfg"
        path.write_text("someOtherKey = 1.0\n")

        assert _parse_ouster_params(path) is None


class TestDiscovery:
    def test_prefers_studio_metadata_over_info(self, tmp_path: Path) -> None:
        (tmp_path / "run.ousterinfo.json").write_text("{}")
        (tmp_path / "run.ousterstudio.json").write_text("{}")

        found = _find_metadata(tmp_path, {"info": "run.ousterinfo.json"})

        assert found == tmp_path / "run.ousterstudio.json"

    def test_falls_back_to_info_metadata(self, tmp_path: Path) -> None:
        (tmp_path / "run.ousterinfo.json").write_text("{}")

        found = _find_metadata(tmp_path, {"info": "run.ousterinfo.json"})

        assert found == tmp_path / "run.ousterinfo.json"

    def test_returns_none_when_no_metadata_exists(self, tmp_path: Path) -> None:
        assert _find_metadata(tmp_path, {"info": "missing.json"}) is None

    def test_finds_nav_file_in_a_nav_subfolder(self, tmp_path: Path) -> None:
        nav_dir = tmp_path / "01_navdata"
        nav_dir.mkdir()
        (nav_dir / "navdata2.daq").write_bytes(b"buf3")

        assert _find_nav_file(tmp_path) == nav_dir / "navdata2.daq"

    def test_finds_nav_file_at_the_dataset_root(self, tmp_path: Path) -> None:
        (tmp_path / "navdata2.daq").write_bytes(b"buf3")

        assert _find_nav_file(tmp_path) == tmp_path / "navdata2.daq"

    def test_returns_none_when_there_is_no_nav_file(self, tmp_path: Path) -> None:
        assert _find_nav_file(tmp_path) is None


class TestConvertOusterDataset:
    def test_requires_a_manifest(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="No data.yaml"):
            convert_ouster_dataset(tmp_path)

    def test_requires_at_least_one_sensor(self, tmp_path: Path) -> None:
        (tmp_path / "data.yaml").write_text("ousterDaqs: []\n")

        with pytest.raises(ValueError, match="No ousterDaqs entries"):
            convert_ouster_dataset(tmp_path)

    def test_skips_sensors_whose_pcap_is_missing(self, tmp_path: Path) -> None:
        (tmp_path / "data.yaml").write_text("ousterDaqs:\n  - pcap: missing.pcap\n    info: missing.json\n")

        # Nothing is decodable, so the converter returns empty rather than reaching ouster-sdk.
        assert convert_ouster_dataset(tmp_path, apply_nav=False) == []
