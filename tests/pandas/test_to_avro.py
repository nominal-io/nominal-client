from pathlib import Path

import pandas as pd
import pytest

import nominal.io
import nominal.pandas  # noqa: F401  (registers df.nom)
from nominal.io import instro

DATA = Path(__file__).parent.parent / "io" / "data"


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["timestamp", "channel"], kind="stable", ignore_index=True)


@pytest.mark.parametrize("fmt", ["csv", "avro", "jsonl"])
def test_to_avro_round_trips(tmp_path: Path, fmt: str):
    """Data written with to_avro reads back unchanged, whichever format it came from."""
    df = nominal.io.read(DATA / f"capture.{fmt}")
    path = df.nom.to_avro(tmp_path / "out.avro")
    pd.testing.assert_frame_equal(_sorted(instro.read_avro(path)), _sorted(df))


def test_to_avro_after_filter(tmp_path: Path):
    """to_avro works on a filtered frame and keeps a numeric value column."""
    df = instro.read_avro(DATA / "capture.avro").nom.select("daq.ai_0").nom.with_tags(run="steady")
    back = instro.read_avro(df.nom.to_avro(tmp_path / "out.avro"))
    pd.testing.assert_frame_equal(back, df)
    assert back["value"].dtype == "float64"


def test_to_avro_splits_long_channels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A channel longer than the record size round-trips through several Avro records."""
    monkeypatch.setattr(instro, "_AVRO_RECORD_SIZE", 7)
    df = instro.read_avro(DATA / "capture.avro")
    back = instro.read_avro(df.nom.to_avro(tmp_path / "out.avro"))
    pd.testing.assert_frame_equal(_sorted(back), _sorted(df))


def test_to_avro_missing_values(tmp_path: Path):
    """Missing values are written as NaN and string samples survive."""
    ts = pd.to_datetime([1, 2, 3], unit="ns", utc=True)
    df = pd.DataFrame({"timestamp": ts, "channel": "a", "value": [1.0, None, "x"], "tags": [{}, {}, {}]})
    back = instro.read_avro(df.nom.to_avro(tmp_path / "out.avro"))
    assert back["value"].iloc[0] == 1.0
    assert pd.isna(back["value"].iloc[1])
    assert back["value"].iloc[2] == "x"
