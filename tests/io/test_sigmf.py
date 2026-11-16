from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import sigmf
from sigmf import SigMFFile

import nominal.io as nio
from nominal.io.schema import COLUMNS
from nominal.io_sigmf import read_sigmf


def sel(df: pd.DataFrame, *channels: str) -> pd.DataFrame:
    """Rows of these channels, with ``value`` re-typed for what is left."""
    rows = df[df["channel"].isin(channels)].reset_index(drop=True)
    return rows.assign(value=pd.Series(rows["value"].tolist()))


def chans(df: pd.DataFrame) -> list[str]:
    return list(df["channel"].unique())


FS = 1000.0
START = pd.Timestamp("2026-01-01T09:00:00Z")


def _write(tmp_path: Path, samples: np.ndarray, datatype: str, channels: int = 1, datetime: str | None = None) -> Path:
    """Write a SigMF recording with two captures and one annotation; return its meta path."""
    data = tmp_path / "rec.sigmf-data"
    samples.tofile(data)
    meta = SigMFFile(
        data_file=str(data),
        global_info={sigmf.DATATYPE_KEY: datatype, sigmf.SAMPLE_RATE_KEY: FS, "core:num_channels": channels},
    )
    first = {"core:frequency": 915e6}
    if datetime:
        first["core:datetime"] = datetime
    meta.add_capture(0, metadata=first)
    meta.add_capture(50, metadata={"core:frequency": 916e6})
    meta.add_annotation(
        20,
        10,
        metadata={"core:label": "burst", "core:comment": "test", "core:freq_lower_edge": 914.9e6},
    )
    path = tmp_path / "rec.sigmf-meta"
    meta.tofile(str(path))
    return path


@pytest.fixture
def complex_path(tmp_path: Path) -> Path:
    n = np.arange(100)
    return _write(tmp_path, (n + 1j * -n).astype(np.complex64), "cf32_le")


def test_iq_channels(complex_path: Path):
    df = read_sigmf(complex_path, start=START)
    assert list(df.columns) == COLUMNS
    assert set(chans(df)) == {"sigmf.I", "sigmf.Q", "sigmf.frequency", "sigmf.annotation"}
    i, q = sel(df, "sigmf.I"), sel(df, "sigmf.Q")
    assert i["value"].tolist() == list(range(100))
    assert q["value"].tolist() == [-v for v in range(100)]
    assert i["tags"].iloc[0] == {"recording": "rec"}
    assert i["timestamp"].iloc[1] - START == pd.Timedelta(milliseconds=1)


def test_frequency_per_capture(complex_path: Path):
    freq = sel(read_sigmf(complex_path, start=START), "sigmf.frequency")
    assert freq["value"].tolist() == [915e6, 916e6]
    assert (freq["timestamp"] - START).tolist() == [pd.Timedelta(0), pd.Timedelta(milliseconds=50)]
    assert freq["tags"].iloc[0] == {"recording": "rec", "units": "Hz"}


def test_annotation_span(complex_path: Path):
    ann = sel(read_sigmf(complex_path, start=START), "sigmf.annotation")
    assert ann["value"].tolist() == ["burst", ""]
    assert (ann["timestamp"] - START).tolist() == [pd.Timedelta(milliseconds=20), pd.Timedelta(milliseconds=30)]
    assert ann["tags"].iloc[0] == {"recording": "rec", "comment": "test", "freq_lower_edge": "914900000.0"}


def test_slice_keeps_recording_times(complex_path: Path):
    df = read_sigmf(complex_path, start=START, start_index=60, count=10)
    i = sel(df, "sigmf.I")
    assert i["value"].tolist() == list(range(60, 70))
    assert i["timestamp"].iloc[0] == START + pd.Timedelta(milliseconds=60)
    # the capture in force at the first sample read, and no annotation (it ended at 30)
    assert sel(df, "sigmf.frequency")["value"].tolist() == [916e6]
    assert "sigmf.annotation" not in chans(df)


def test_numeric_without_annotations(complex_path: Path):
    df = read_sigmf(complex_path, start=START, start_index=60)
    assert df["value"].dtype == "float64"


def test_real_and_multichannel(tmp_path: Path):
    (tmp_path / "real").mkdir()
    (tmp_path / "multi").mkdir()
    real = read_sigmf(_write(tmp_path / "real", np.arange(100, dtype=np.float32), "rf32_le"), start=START)
    assert "sigmf.value" in chans(real)
    interleaved = np.arange(200).astype(np.float32).view(np.complex64)  # 2 channels x 50 samples
    multi = read_sigmf(_write(tmp_path / "multi", interleaved, "cf32_le", channels=2), start=START, prefix=None)
    assert {"ch0.I", "ch0.Q", "ch1.I", "ch1.Q"} <= set(chans(multi))
    assert sel(multi, "ch1.I")["value"].iloc[0] == 2.0


def test_capture_datetime_is_the_start(tmp_path: Path):
    path = _write(tmp_path, np.zeros(100, np.complex64), "cf32_le", datetime="2020-12-20T17:32:07.142626Z")
    df = read_sigmf(path)
    assert df["timestamp"].iloc[0] == pd.Timestamp("2020-12-20T17:32:07.142626Z")


def test_no_start_warns_and_uses_now(complex_path: Path):
    before = pd.Timestamp.now(tz="UTC")
    with pytest.warns(UserWarning, match="no start time"):
        df = read_sigmf(complex_path)
    assert before <= df["timestamp"].iloc[0] <= pd.Timestamp.now(tz="UTC")


def test_paths_and_dispatch(complex_path: Path):
    by_name = read_sigmf(complex_path.with_suffix(""), start=START)
    by_data = read_sigmf(complex_path.with_suffix(".sigmf-data"), start=START)
    pd.testing.assert_frame_equal(by_name, by_data)
    with pytest.warns(UserWarning):
        assert "sigmf.I" in chans(nio.read(complex_path))


def test_registered_and_contract(complex_path: Path):
    from nominal.io.testing import check_reader
    from nominal.io_sigmf import SPEC

    assert nio.resolve_format(complex_path).name == "sigmf"
    assert nio.resolve_format(complex_path.with_suffix(".sigmf-data")).name == "sigmf"
    with pytest.warns(UserWarning, match="no start time"):
        check_reader(SPEC, complex_path)


def test_chunk_and_polars_engine(complex_path: Path):
    from nominal.io_sigmf import read_sigmf_chunk

    chunk = read_sigmf_chunk(complex_path, start=START)
    assert set(chunk.channel_names) == {"sigmf.I", "sigmf.Q", "sigmf.frequency", "sigmf.annotation"}
    df = read_sigmf(complex_path, start=START, engine="polars")
    assert df.height == len(chunk)
