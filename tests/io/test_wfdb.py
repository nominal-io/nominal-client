from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import nominal.io as nio
from nominal.io.schema import COLUMNS
from nominal.io_wfdb import read_wfdb


def sel(df: pd.DataFrame, *channels: str) -> pd.DataFrame:
    """Rows of these channels, with ``value`` re-typed for what is left."""
    rows = df[df["channel"].isin(channels)].reset_index(drop=True)
    return rows.assign(value=pd.Series(rows["value"].tolist()))


def chans(df: pd.DataFrame) -> list[str]:
    return list(df["channel"].unique())


RECORD = Path(__file__).parent / "data" / "wfdb" / "119"
START = pd.Timestamp("2026-01-01T09:00:00Z")
FS = 360


@pytest.fixture
def frame() -> pd.DataFrame:
    return read_wfdb(RECORD, start=START)


def test_columns(frame: pd.DataFrame):
    """A WFDB record reads into the long-form columns with UTC timestamps in order."""
    assert list(frame.columns) == COLUMNS
    assert str(frame["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert frame["timestamp"].is_monotonic_increasing


def test_signals(frame: pd.DataFrame):
    """Each lead becomes a numeric channel tagged with its record and units, at the record's sample rate."""
    lead = sel(frame, "wfdb.MLII")
    assert len(lead) == 60 * FS
    assert lead["value"].dtype == "float64"
    assert lead["tags"].iloc[0] == {"record": "119", "units": "mV"}
    assert lead["timestamp"].iloc[0] == START
    assert lead["timestamp"].iloc[1] - START == pd.Timedelta(round(1e9 / FS), "ns")


def test_annotations(frame: pd.DataFrame):
    """Beat and rhythm annotations become string channels with the expected counts."""
    assert set(chans(frame)) == {"wfdb.MLII", "wfdb.V1", "wfdb.beat", "wfdb.rhythm"}
    beats = sel(frame, "wfdb.beat")["value"].value_counts().to_dict()
    assert beats == {"N": 46, "V": 19}
    rhythm = sel(frame, "wfdb.rhythm")["value"].tolist()
    assert rhythm[:2] == ["N", "B"]
    assert sel(frame, "wfdb.beat")["tags"].iloc[0] == {"record": "119"}


def test_beats_fall_on_samples(frame: pd.DataFrame):
    """Every beat annotation lands on a timestamp that also has a signal sample."""
    beat_times = set(sel(frame, "wfdb.beat")["timestamp"])
    signal_times = set(sel(frame, "wfdb.MLII")["timestamp"])
    assert beat_times <= signal_times


def test_no_annotations():
    """annotations=None reads only the signals, as floats."""
    df = read_wfdb(RECORD, start=START, annotations=None)
    assert chans(df) == ["wfdb.MLII", "wfdb.V1"]
    assert df["value"].dtype == "float64"


def test_missing_annotation_file_is_skipped():
    """A missing annotation file is skipped instead of failing the read."""
    df = read_wfdb(RECORD, start=START, annotations="nope")
    assert chans(df) == ["wfdb.MLII", "wfdb.V1"]


def test_prefix():
    """The channel prefix can be changed or removed."""
    assert "ecg.MLII" in chans(read_wfdb(RECORD, start=START, prefix="ecg"))
    assert "MLII" in chans(read_wfdb(RECORD, start=START, prefix=None))


def test_slice_keeps_record_times(frame: pd.DataFrame):
    """Reading part of a record keeps its absolute times and matches the same span of a full read."""
    part = read_wfdb(RECORD, start=START, sampfrom=10 * FS, sampto=20 * FS)
    lead = sel(part, "wfdb.MLII")
    assert len(lead) == 10 * FS
    assert lead["timestamp"].iloc[0] == START + pd.Timedelta(seconds=10)
    window = (frame["timestamp"] >= START + pd.Timedelta(seconds=10)) & (
        frame["timestamp"] < START + pd.Timedelta(seconds=20)
    )
    expected = frame[window]
    pd.testing.assert_frame_equal(
        sel(part, "wfdb.MLII", "wfdb.beat").reset_index(drop=True),
        sel(expected, "wfdb.MLII", "wfdb.beat").reset_index(drop=True),
    )


def test_hea_extension_and_channels():
    """A path ending in .hea works, and channels= selects signals by index."""
    df = read_wfdb(f"{RECORD}.hea", start=START, channels=[1], annotations=None)
    assert chans(df) == ["wfdb.V1"]


def test_no_start_warns_and_uses_now():
    """With no start in the header or arguments, the reader warns and uses the current time."""
    before = pd.Timestamp.now(tz="UTC")
    with pytest.warns(UserWarning, match="no start time"):
        df = read_wfdb(RECORD)
    assert before <= df["timestamp"].iloc[0] <= pd.Timestamp.now(tz="UTC")


def test_registered_and_dispatched_by_extension():
    """The .hea extension resolves to the wfdb format with its install hint."""
    spec = nio.resolve_format(f"{RECORD}.hea")
    assert spec.name == "wfdb" and spec.install_hint == "pip install nominal[wfdb]"


def test_reader_contract():
    """The wfdb reader passes the reader contract check on the sample record."""
    from nominal.io.testing import check_reader
    from nominal.io_wfdb import SPEC

    with pytest.warns(UserWarning, match="no start time"):
        check_reader(SPEC, f"{RECORD}.hea")


def test_read_dispatches_hea():
    """nominal.io.read reads a .hea file and includes the beat channel."""
    with pytest.warns(UserWarning):
        df = nio.read(f"{RECORD}.hea")
    assert "wfdb.beat" in chans(df)


def test_chunk_and_polars_engine():
    """The chunk function and the polars engine read the same samples."""
    from nominal.io_wfdb import read_wfdb_chunk

    chunk = read_wfdb_chunk(RECORD, start=START)
    assert set(chunk.channel_names) == {"wfdb.MLII", "wfdb.V1", "wfdb.beat", "wfdb.rhythm"}
    df = read_wfdb(RECORD, start=START, engine="polars")
    assert df.height == len(chunk) and "value_str" in df.columns
