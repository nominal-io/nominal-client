"""The TDMS-to-DataFrame path behind upload_tdms, on files written by the fixture writer."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from nominal import ts
from nominal.tdms import NiTimestamp
from nominal.tdms._tdms import _tdms_to_dataframe
from tests.tdms.tdms_write import Writer, channel_object, root_object

UNIX_MINUS_NI_EPOCH_S = 2_082_844_800
START_S = 1_709_251_200  # 2024-03-01T00:00:00Z
START_NS = START_S * 1_000_000_000


def ni_timestamp(epoch_seconds: int) -> NiTimestamp:
    return NiTimestamp(epoch_seconds + UNIX_MINUS_NI_EPOCH_S, 0)


def waveform(group: str, name: str, data, offset: float | None = None, **extra):
    properties = {"wf_start_time": ni_timestamp(START_S), "wf_increment": 0.1, **extra}
    if offset is not None:
        properties["wf_start_offset"] = offset
    return channel_object(group, name, np.asarray(data), properties)


def write(path: Path, *objects) -> Path:
    with Writer(path) as writer:
        writer.write_segment([root_object({"name": path.stem}), *objects])
    return path


class TestWaveformMode:
    @pytest.fixture
    def frame(self, tmp_path):
        path = write(
            tmp_path / "wave.tdms",
            waveform("Engine Bay", "Voltage", np.arange(5, dtype=np.float64)),
            waveform("Engine Bay", "Current", np.arange(5, dtype=np.int32) * 2),
            waveform("Engine Bay", "Late", np.arange(5, dtype=np.float64), offset=2.5),
            waveform("Engine Bay", "Label", np.array(["a", "b", "c", "d", "e"], dtype=object)),
            waveform(
                "Engine Bay", "Captured", np.array([START_NS + i * 10**9 for i in range(5)], dtype="datetime64[ns]")
            ),
            channel_object("Engine Bay", "Untimed", np.arange(5, dtype=np.float64)),
            waveform("Engine Bay", "Complex", np.arange(5, dtype=np.complex128)),
        )
        column, kind, df = _tdms_to_dataframe(path)
        assert (column, kind) == ("__nominal_ts__", ts.EPOCH_NANOSECONDS)
        return df

    def test_channels_are_named_group_dot_channel_with_spaces_underscored(self, frame):
        assert set(frame.columns) - {"__nominal_ts__"} == {
            "Engine_Bay.Voltage",
            "Engine_Bay.Current",
            "Engine_Bay.Late",
            "Engine_Bay.Label",
            "Engine_Bay.Captured",
        }

    def test_timestamps_are_int64_epoch_nanoseconds_from_the_waveform_properties(self, frame):
        stamps = frame["__nominal_ts__"]
        assert stamps.dtype == np.int64
        aligned = frame.dropna(subset=["Engine_Bay.Voltage"])
        assert list(aligned["__nominal_ts__"]) == [START_NS + i * 100_000_000 for i in range(5)]

    def test_a_start_offset_shifts_the_channel(self, frame):
        late = frame.dropna(subset=["Engine_Bay.Late"])
        assert late["__nominal_ts__"].iloc[0] == START_NS + 2_500_000_000

    def test_values_keep_their_types(self, frame):
        aligned = frame.dropna(subset=["Engine_Bay.Voltage"])
        assert list(aligned["Engine_Bay.Voltage"]) == [0.0, 1.0, 2.0, 3.0, 4.0]
        assert list(aligned["Engine_Bay.Current"]) == [0, 2, 4, 6, 8]
        assert list(aligned["Engine_Bay.Label"]) == ["a", "b", "c", "d", "e"]

    def test_timestamp_data_channels_become_datetime64(self, frame):
        captured = frame.dropna(subset=["Engine_Bay.Captured"])["Engine_Bay.Captured"]
        assert captured.dtype == "datetime64[ns]"
        assert captured.iloc[1] == pd.Timestamp(START_NS + 10**9)

    def test_channels_without_waveform_properties_or_a_representation_are_skipped(self, frame, caplog):
        assert "Engine_Bay.Untimed" not in frame.columns
        assert "Engine_Bay.Complex" not in frame.columns


class TestTimeColumnMode:
    def test_each_group_is_indexed_by_its_own_time_channel(self, tmp_path):
        path = write(
            tmp_path / "explicit.tdms",
            channel_object("Sensors", "Time", np.arange(4) * 0.5 + START_S),
            channel_object("Sensors", "Pressure", np.arange(4) * 10.0),
            channel_object("Sensors", "Short", np.arange(3) * 1.0),
            channel_object("Untimed", "Orphan", np.arange(4) * 1.0),
        )
        column, kind, df = _tdms_to_dataframe(path, "Time", ts.EPOCH_SECONDS)
        assert (column, kind) == ("Time", ts.EPOCH_SECONDS)
        assert list(df.columns) == ["Time", "Sensors.Pressure"]
        assert list(df["Time"]) == pytest.approx([START_S + i * 0.5 for i in range(4)])
        assert list(df["Sensors.Pressure"]) == [0.0, 10.0, 20.0, 30.0]

    def test_a_tdms_timestamp_time_channel_indexes_as_datetime64(self, tmp_path):
        stamps = np.array([START_NS + i * 10**9 for i in range(3)], dtype="datetime64[ns]")
        path = write(
            tmp_path / "stamped.tdms",
            channel_object("G", "Time", stamps),
            channel_object("G", "Value", np.arange(3) * 1.0),
        )
        _, _, df = _tdms_to_dataframe(path, "Time", ts.ISO_8601)
        assert df["Time"].dtype == "datetime64[ns]"
        assert df["Time"].iloc[2] == pd.Timestamp(START_NS + 2 * 10**9)

    def test_both_or_neither_timestamp_arguments(self, tmp_path):
        path = write(tmp_path / "x.tdms", channel_object("G", "Time", np.arange(2) * 1.0))
        with pytest.raises(ValueError, match="either both, or neither"):
            _tdms_to_dataframe(path, "Time", None)
