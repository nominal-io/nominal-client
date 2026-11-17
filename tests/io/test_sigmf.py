from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

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


def _write(
    tmp_path: Path,
    samples: np.ndarray[Any, Any],
    datatype: str,
    channels: int = 1,
    datetime: str | None = None,
    checksum: str | None = None,
) -> Path:
    """Write a SigMF recording with two captures and one annotation; return its meta path."""
    data = tmp_path / "rec.sigmf-data"
    samples.tofile(data)
    first: dict[str, Any] = {"core:sample_start": 0, "core:frequency": 915e6}
    if datetime:
        first["core:datetime"] = datetime
    meta = {
        "global": {
            "core:datatype": datatype,
            "core:sample_rate": FS,
            "core:num_channels": channels,
            "core:version": "1.0.0",
            **({"core:sha512": checksum} if checksum else {}),
        },
        "captures": [first, {"core:sample_start": 50, "core:frequency": 916e6}],
        "annotations": [
            {
                "core:sample_start": 20,
                "core:sample_count": 10,
                "core:label": "burst",
                "core:comment": "test",
                "core:freq_lower_edge": 914.9e6,
            }
        ],
    }
    path = tmp_path / "rec.sigmf-meta"
    path.write_text(json.dumps(meta))
    return path


@pytest.fixture
def complex_path(tmp_path: Path) -> Path:
    n = np.arange(100)
    return _write(tmp_path, (n + 1j * -n).astype(np.complex64), "cf32_le")


def test_iq_channels(complex_path: Path):
    """A complex recording gives I and Q channels, a frequency channel and an annotation channel."""
    df = read_sigmf(complex_path, start=START)
    assert list(df.columns) == COLUMNS
    assert set(chans(df)) == {"sigmf.I", "sigmf.Q", "sigmf.frequency", "sigmf.annotation"}
    i, q = sel(df, "sigmf.I"), sel(df, "sigmf.Q")
    assert i["value"].tolist() == list(range(100))
    assert q["value"].tolist() == [-v for v in range(100)]
    assert i["tags"].iloc[0] == {"recording": "rec"}
    assert i["timestamp"].iloc[1] - START == pd.Timedelta(milliseconds=1)


def test_frequency_per_capture(complex_path: Path):
    """Each capture's centre frequency is recorded at the capture's first sample."""
    freq = sel(read_sigmf(complex_path, start=START), "sigmf.frequency")
    assert freq["value"].tolist() == [915e6, 916e6]
    assert (freq["timestamp"] - START).tolist() == [pd.Timedelta(0), pd.Timedelta(milliseconds=50)]
    assert freq["tags"].iloc[0] == {"recording": "rec", "units": "Hz"}


def test_annotation_span(complex_path: Path):
    """An annotation is its label at its first sample and an empty string after its last."""
    ann = sel(read_sigmf(complex_path, start=START), "sigmf.annotation")
    assert ann["value"].tolist() == ["burst", ""]
    assert (ann["timestamp"] - START).tolist() == [pd.Timedelta(milliseconds=20), pd.Timedelta(milliseconds=30)]
    assert ann["tags"].iloc[0] == {"recording": "rec", "comment": "test", "freq_lower_edge": "914900000.0"}


def test_slice_keeps_recording_times(complex_path: Path):
    """Reading part of a recording keeps its absolute times and the capture in force."""
    df = read_sigmf(complex_path, start=START, start_index=60, count=10)
    i = sel(df, "sigmf.I")
    assert i["value"].tolist() == list(range(60, 70))
    assert i["timestamp"].iloc[0] == START + pd.Timedelta(milliseconds=60)
    # the capture in force at the first sample read, and no annotation (it ended at 30)
    assert sel(df, "sigmf.frequency")["value"].tolist() == [916e6]
    assert "sigmf.annotation" not in chans(df)


def test_numeric_without_annotations(complex_path: Path):
    """A slice with no annotations still gives a float64 value column."""
    df = read_sigmf(complex_path, start=START, start_index=60)
    assert df["value"].dtype == "float64"


def test_real_and_multichannel(tmp_path: Path):
    """Real recordings give a value channel, and multi-channel ones name each channel."""
    (tmp_path / "real").mkdir()
    (tmp_path / "multi").mkdir()
    real = read_sigmf(_write(tmp_path / "real", np.arange(100, dtype=np.float32), "rf32_le"), start=START)
    assert "sigmf.value" in chans(real)
    interleaved = np.arange(200).astype(np.float32).view(np.complex64)  # 2 channels x 50 samples
    multi = read_sigmf(_write(tmp_path / "multi", interleaved, "cf32_le", channels=2), start=START, prefix=None)
    assert {"ch0.I", "ch0.Q", "ch1.I", "ch1.Q"} <= set(chans(multi))
    assert sel(multi, "ch1.I")["value"].iloc[0] == 2.0


def test_capture_datetime_is_the_start(tmp_path: Path):
    """The first capture's datetime is the start time when none is passed."""
    path = _write(tmp_path, np.zeros(100, np.complex64), "cf32_le", datetime="2020-12-20T17:32:07.142626Z")
    df = read_sigmf(path)
    assert df["timestamp"].iloc[0] == pd.Timestamp("2020-12-20T17:32:07.142626Z")


def test_no_start_warns_and_uses_now(complex_path: Path):
    """With no start anywhere, the reader warns and uses the current time."""
    before = pd.Timestamp.now(tz="UTC")
    with pytest.warns(UserWarning, match="no start time"):
        df = read_sigmf(complex_path)
    assert before <= df["timestamp"].iloc[0] <= pd.Timestamp.now(tz="UTC")


def test_paths_and_dispatch(complex_path: Path):
    """A recording is found by its meta path, data path or bare name, and by nominal.io.read."""
    by_name = read_sigmf(complex_path.with_suffix(""), start=START)
    by_data = read_sigmf(complex_path.with_suffix(".sigmf-data"), start=START)
    pd.testing.assert_frame_equal(by_name, by_data)
    with pytest.warns(UserWarning):
        assert "sigmf.I" in chans(nio.read(complex_path))


def test_registered_and_contract(complex_path: Path):
    """The sigmf format is registered for each SigMF extension and passes the reader contract."""
    from nominal.io.testing import check_reader
    from nominal.io_sigmf import SPEC

    assert nio.resolve_format(complex_path).name == "sigmf"
    assert nio.resolve_format(complex_path.with_suffix(".sigmf-data")).name == "sigmf"
    with pytest.warns(UserWarning, match="no start time"):
        check_reader(SPEC, complex_path)


def test_chunk_and_polars_engine(complex_path: Path):
    """The chunk function and the polars engine read the same samples."""
    from nominal.io_sigmf import read_sigmf_chunk

    chunk = read_sigmf_chunk(complex_path, start=START)
    assert set(chunk.channel_names) == {"sigmf.I", "sigmf.Q", "sigmf.frequency", "sigmf.annotation"}
    df = read_sigmf(complex_path, start=START, engine="polars")
    assert df.height == len(chunk)


def test_empty_selections_give_an_empty_frame(complex_path: Path):
    """count=0, and a start at or past the end of the recording, read as no rows instead of raising."""
    assert read_sigmf(complex_path, start=START, count=0).empty
    assert read_sigmf(complex_path, start=START, start_index=60, count=0).empty
    assert read_sigmf(complex_path, start=START, start_index=100).empty
    assert read_sigmf(complex_path, start=START, start_index=500).empty


def test_slice_inside_an_annotation_keeps_its_label(complex_path: Path):
    """A slice that starts inside an annotation still carries its label, from the first sample read."""
    ann = sel(read_sigmf(complex_path, start=START, start_index=25, count=20), "sigmf.annotation")
    assert ann["value"].tolist() == ["burst", ""]
    assert (ann["timestamp"] - START).tolist() == [pd.Timedelta(milliseconds=25), pd.Timedelta(milliseconds=30)]
    inside = sel(read_sigmf(complex_path, start=START, start_index=22, count=5), "sigmf.annotation")
    assert inside["value"].tolist() == ["burst"]  # the annotation outlasts the slice, so there is no closing mark


def test_reads_without_the_sigmf_library():
    """The reader needs only the standard library and numpy, not the sigmf package."""
    import subprocess
    import sys

    code = "import sys, nominal.io_sigmf; sys.exit('sigmf' in sys.modules)"
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


def test_archive_is_read_like_its_files(complex_path: Path, tmp_path: Path):
    """A .sigmf archive (a tar of the meta and data files) reads the same as the loose files."""
    archive = tmp_path / "rec.sigmf"
    with tarfile.open(archive, "w") as tar:
        tar.add(complex_path, arcname="rec/rec.sigmf-meta")
        tar.add(complex_path.with_suffix(".sigmf-data"), arcname="rec/rec.sigmf-data")
    pd.testing.assert_frame_equal(read_sigmf(archive, start=START), read_sigmf(complex_path, start=START))
    with pytest.raises(FileNotFoundError):
        read_sigmf(tmp_path / "nothing.sigmf-meta", start=START)


def test_checksum_is_verified_unless_skipped(tmp_path: Path):
    """A data file that doesn't match core:sha512 raises, unless skip_checksum is set."""
    samples = (np.arange(100) + 1j * np.arange(100)).astype(np.complex64)
    good = hashlib.sha512(samples.tobytes()).hexdigest()
    ok = _write(tmp_path, samples, "cf32_le", checksum=good)
    assert not read_sigmf(ok, start=START).empty
    bad = _write(tmp_path, samples, "cf32_le", checksum="0" * 128)
    with pytest.raises(ValueError, match="core:sha512"):
        read_sigmf(bad, start=START)
    assert not read_sigmf(bad, start=START, skip_checksum=True).empty


@pytest.mark.parametrize(
    ("datatype", "dtype", "complex_"),
    [("ci16_le", "<i2", True), ("cu8", "u1", True), ("ri32_be", ">i4", False), ("rf64_le", "<f8", False)],
)
def test_other_datatypes(tmp_path: Path, datatype: str, dtype: str, complex_: bool):
    """Integer, unsigned, big-endian and double datatypes read as numbers."""
    raw = np.arange(1, 201 if complex_ else 101).astype(dtype)
    df = read_sigmf(_write(tmp_path, raw, datatype), start=START)
    first = "sigmf.I" if complex_ else "sigmf.value"
    expected = raw[::2] if complex_ else raw
    assert sel(df, first)["value"].tolist() == expected.astype(float).tolist()
    assert ("sigmf.Q" in chans(df)) == complex_


def test_multichannel_samples_are_interleaved(tmp_path: Path):
    """Samples of several channels are read interleaved, one column per channel."""
    interleaved = np.arange(200, dtype=np.float32)  # real, 2 channels x 100 samples
    df = read_sigmf(_write(tmp_path, interleaved, "rf32_le", channels=2), start=START, prefix=None)
    assert sel(df, "ch0.value")["value"].tolist() == interleaved[0::2].tolist()
    assert sel(df, "ch1.value")["value"].tolist() == interleaved[1::2].tolist()


def test_bad_metadata_is_reported(tmp_path: Path):
    """An unsupported datatype, a missing required field, or header bytes raise a clear ValueError."""
    path = _write(tmp_path, np.zeros(10, np.float32), "rf32_le")
    meta = json.loads(path.read_text())
    meta["global"]["core:datatype"] = "xx7"
    path.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="unsupported SigMF datatype"):
        read_sigmf(path, start=START)
    del meta["global"]["core:sample_rate"]
    meta["global"]["core:datatype"] = "rf32_le"
    path.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="core:sample_rate"):
        read_sigmf(path, start=START)
    meta["global"]["core:sample_rate"] = FS
    meta["captures"][0]["core:header_bytes"] = 16
    path.write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="header_bytes"):
        read_sigmf(path, start=START)
