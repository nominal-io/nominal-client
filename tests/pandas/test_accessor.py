from pathlib import Path

import pandas as pd
import pytest

import nominal.io
import nominal.pandas  # noqa: F401  (registers df.nom)
from nominal.io import instro
from nominal.io.schema import COLUMNS

DATA = Path(__file__).parent.parent / "io" / "data"


@pytest.fixture
def frame() -> pd.DataFrame:
    return instro.read_avro(DATA / "capture.avro")


def test_requires_long_form_columns():
    """df.nom needs the long-form columns and says which are missing."""
    with pytest.raises(AttributeError, match="missing"):
        pd.DataFrame({"x": [1]}).nom


def test_channels(frame: pd.DataFrame):
    """Channels lists channel names in order of first appearance."""
    assert frame.nom.channels == [
        "psu.voltage_setpoint",
        "daq.state",
        "daq.ai_0",
        "daq.ai_1",
        "daq.temperature",
    ]


def test_select(frame: pd.DataFrame):
    """Select keeps only the named channels, resets the index and re-types a numeric value column."""
    df = frame.nom.select("daq.ai_0", "daq.temperature")
    assert list(df.columns) == COLUMNS
    assert set(df["channel"]) == {"daq.ai_0", "daq.temperature"}
    assert df["value"].dtype == "float64"  # string channel filtered out
    assert df.index.equals(pd.RangeIndex(len(df)))


def test_select_unknown_channel(frame: pd.DataFrame):
    """Select raises KeyError for a channel that isn't in the frame."""
    with pytest.raises(KeyError, match="nope"):
        frame.nom.select("nope")


def test_between(frame: pd.DataFrame):
    """Between keeps rows with start <= timestamp < end, and no arguments keeps everything."""
    start = frame["timestamp"].iloc[0]
    df = frame.nom.between(start + pd.Timedelta("1s"), start + pd.Timedelta("1.5s"))
    assert df["timestamp"].min() == start + pd.Timedelta("1s")
    assert df["timestamp"].max() < start + pd.Timedelta("1.5s")
    assert frame.nom.between().equals(frame)


def test_between_naive_is_utc(frame: pd.DataFrame):
    """A time without a zone is read as UTC."""
    assert frame.nom.between("2025-10-09 08:53:21").equals(frame.nom.between("2025-10-09 08:53:21Z"))


def test_with_tags(frame: pd.DataFrame):
    """with_tags keeps rows whose tags include every given key and value."""
    df = frame.nom.with_tags(run="steady")
    assert all(t == {"run": "steady"} for t in df["tags"])
    assert "psu.voltage_setpoint" not in df.nom.channels  # commands are untagged


def test_chain(frame: pd.DataFrame):
    """Accessor methods chain and keep returning long-form frames."""
    df = frame.nom.select("daq.ai_0").nom.with_tags(run="warmup").nom.between(end="2025-10-09 08:53:20.5Z")
    assert len(df) == 50
    assert list(df.columns) == COLUMNS


def test_wide(frame: pd.DataFrame):
    """Wide pivots to one column per channel indexed by timestamp, with numeric channels as float64."""
    wide = frame.nom.wide()
    assert wide.index.name == "timestamp"
    assert set(wide.columns) == set(frame.nom.channels)
    assert wide["daq.ai_0"].dtype == "float64"
    assert wide["daq.state"].dropna().tolist() == ["idle", "running"]
    assert wide["daq.ai_0"].notna().sum() == 200


def test_between_durations(frame: pd.DataFrame):
    """Between accepts a duration at either end, as strings or timedeltas."""
    t0, t1 = frame["timestamp"].min(), frame["timestamp"].max()
    one_second = pd.Timedelta("1s")
    assert frame.nom.between(t0, "1s").equals(frame.nom.between(t0, t0 + one_second))
    assert frame.nom.between("1s", t1).equals(frame.nom.between(t1 - one_second, t1))
    assert frame.nom.between(t0, one_second).equals(frame.nom.between(t0, "1000ms"))
    assert frame.nom.between(t0, "0.5s 500ms").equals(frame.nom.between(t0, "1s"))


def test_between_duration_needs_a_time(frame: pd.DataFrame):
    """A duration needs a time at the other end, and two durations are rejected."""
    with pytest.raises(ValueError, match="start time"):
        frame.nom.between(end="1s")
    with pytest.raises(ValueError, match="both"):
        frame.nom.between("1s", "2s")


def test_time_strings_are_not_durations(frame: pd.DataFrame):
    # pandas reads "08:53:21" as a timedelta; between treats it as a time, like any non-duration string
    """A clock-time string is read as a time, not as a duration."""
    assert frame.nom.between("2025-10-09 08:53:21").equals(frame.nom.between("2025-10-09 08:53:21Z"))


def test_first_and_last(frame: pd.DataFrame):
    """First and last keep the rows within a duration of the first or last timestamp."""
    t0, t1 = frame["timestamp"].min(), frame["timestamp"].max()
    first = frame.nom.first("1s")
    assert first["timestamp"].min() == t0 and first["timestamp"].max() < t0 + pd.Timedelta("1s")
    last = frame.nom.last("1s")
    assert last["timestamp"].max() == t1 and last["timestamp"].min() >= t1 - pd.Timedelta("1s")
    assert list(first.columns) == COLUMNS
    assert frame.nom.select("daq.ai_0").nom.first("100ms").nom.channels == ["daq.ai_0"]


def test_first_needs_a_duration(frame: pd.DataFrame):
    """First rejects a value that isn't a duration."""
    with pytest.raises(ValueError, match="duration"):
        frame.nom.first("2025-10-09")


def test_first_and_last_of_empty_frame(frame: pd.DataFrame):
    """First and last of an empty frame return an empty frame."""
    empty = frame.nom.with_tags(run="nope")
    assert empty.nom.first("1s").empty and empty.nom.last("1s").empty


def test_with_tags_on_an_empty_frame(frame: pd.DataFrame):
    """with_tags on a frame with no rows returns an empty frame instead of raising."""
    empty = frame.iloc[0:0]
    assert empty.nom.with_tags(run="steady").empty
