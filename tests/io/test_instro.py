from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

import nominal.io as nio
from nominal.io import instro
from nominal.io.schema import COLUMNS
from nominal.io.testing import check_reader

DATA = Path(__file__).parent / "data"
FORMATS = {"csv": instro.CSV_SPEC, "avro": instro.AVRO_SPEC, "jsonl": instro.JSONL_SPEC}


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["timestamp", "channel"], kind="stable", ignore_index=True)


@pytest.fixture(params=FORMATS)
def frame(request: pytest.FixtureRequest) -> pd.DataFrame:
    return nio.read(DATA / f"capture.{request.param}")


def test_registered_through_entry_points() -> None:
    """The three instro formats are discovered through nominal's own entry points."""
    names = {f.name for f in nio.list_formats()}
    assert {"instro-csv", "instro-avro", "instro-jsonl"} <= names


@pytest.mark.parametrize("fmt", FORMATS)
def test_contract(fmt: str) -> None:
    """Each instro reader passes the reader contract check on the sample capture."""
    check_reader(FORMATS[fmt], DATA / f"capture.{fmt}")


def test_columns_and_dtypes(frame: pd.DataFrame) -> None:
    """Every instro format reads into the long-form columns, with UTC timestamps in order."""
    assert list(frame.columns) == COLUMNS
    assert str(frame["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert frame["value"].dtype == object  # daq.state is a string channel
    assert frame["timestamp"].is_monotonic_increasing


def test_contents(frame: pd.DataFrame) -> None:
    """The sample capture reads with the expected channels, counts, values and tags."""
    assert frame["channel"].value_counts().to_dict() == {
        "daq.ai_0": 200,
        "daq.ai_1": 200,
        "daq.temperature": 2,
        "daq.state": 2,
        "psu.voltage_setpoint": 2,
    }
    first = frame.iloc[0]
    assert first["timestamp"] == pd.Timestamp(1_760_000_000_000_000_000, unit="ns", tz="UTC")
    assert first["channel"] == "psu.voltage_setpoint"
    assert first["value"] == 5.0
    assert first["tags"] == {}
    assert frame.loc[frame["channel"] == "daq.state", "value"].tolist() == ["idle", "running"]
    assert frame.loc[frame["channel"] == "daq.ai_0", "tags"].iloc[-1] == {"run": "steady"}


def test_formats_agree() -> None:
    """The CSV, Avro and JSON Lines copies of the capture read as identical frames."""
    frames = [nio.read(DATA / f"capture.{fmt}") for fmt in FORMATS]
    for other in frames[1:]:
        pd.testing.assert_frame_equal(frames[0], other)


@pytest.mark.parametrize("fmt", FORMATS)
def test_chunked_equals_whole(fmt: str) -> None:
    """Reading in small chunks and joining them gives the same data as reading whole."""
    path = DATA / f"capture.{fmt}"
    chunks = list(nio.read_chunks(path, engine="pandas", chunksize=50))
    assert len(chunks) > 1
    pd.testing.assert_frame_equal(_sorted(pd.concat(chunks, ignore_index=True)), _sorted(nio.read(path)))


def test_dispatch_picks_instro_format_by_extension() -> None:
    """Each instro extension resolves to its own format."""
    for fmt, spec in FORMATS.items():
        assert nio.resolve_format(DATA / f"capture.{fmt}") == spec


def test_csv_sniff_rejects_other_csv(tmp_path: Path) -> None:
    """The instro sniffers reject a CSV that isn't an instro capture."""
    path = tmp_path / "other.csv"
    path.write_text("a,b\n1,2\n")
    assert instro.CsvReader().sniff(path) is False
    assert instro.AvroReader().sniff(path) is False
    assert instro.JsonlReader().sniff(path) is False


def test_all_numeric_values_are_float(tmp_path: Path) -> None:
    """CSV values that all parse as numbers, including nan and empty, give a float column."""
    path = tmp_path / "numbers.csv"
    path.write_text("timestamp,channel,value,tags\n1,a,1.5,\n2,a,nan,\n3,a,,\n")
    df = instro.read_csv(path)
    assert df["value"].dtype == "float64"
    assert df["value"].isna().tolist() == [False, True, True]


def test_jsonl_null_is_nan(tmp_path: Path) -> None:
    """A JSON null sample reads as NaN in a float column."""
    path = tmp_path / "nulls.jsonl"
    path.write_text('{"channel_data": {"a": [null, 1.0]}, "timestamps": [1, 2], "tags": null}\n')
    df = instro.read_jsonl(path)
    assert df["value"].dtype == "float64"
    assert df["value"].isna().tolist() == [True, False]


@pytest.mark.parametrize("fmt", FORMATS)
def test_write_avro_round_trips(tmp_path: Path, fmt: str) -> None:
    """Data written with write_avro reads back unchanged."""
    df = nio.read(DATA / f"capture.{fmt}")
    path = instro.write_avro(df, tmp_path / "out.avro")
    pd.testing.assert_frame_equal(_sorted(instro.read_avro(path)), _sorted(df))


def test_write_avro_splits_long_channels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A channel longer than the record size is split into several records and reads back whole."""
    monkeypatch.setattr(instro, "_AVRO_RECORD_SIZE", 7)
    df = instro.read_avro(DATA / "capture.avro")
    back = instro.read_avro(instro.write_avro(df, tmp_path / "out.avro"))
    pd.testing.assert_frame_equal(_sorted(back), _sorted(df))


def test_write_avro_missing_values(tmp_path: Path) -> None:
    """Missing values are written as NaN and string samples survive."""
    ts = pd.to_datetime([1, 2, 3], unit="ns", utc=True)
    df = pd.DataFrame({"timestamp": ts, "channel": "a", "value": [1.0, None, "x"], "tags": [{}, {}, {}]})
    back = instro.read_avro(instro.write_avro(df, tmp_path / "out.avro"))
    assert back["value"].iloc[0] == 1.0
    assert pd.isna(back["value"].iloc[1])
    assert back["value"].iloc[2] == "x"


def test_write_avro_rejects_non_long_form(tmp_path: Path) -> None:
    """write_avro rejects a frame that isn't long-form."""
    with pytest.raises(ValueError, match="expected columns"):
        instro.write_avro(pd.DataFrame({"x": [1]}), tmp_path / "out.avro")


@pytest.mark.parametrize("fmt", FORMATS)
def test_polars_engine(fmt: str) -> None:
    """Each instro format reads into the polars layout with its string samples in value_str."""
    df = nio.read(DATA / f"capture.{fmt}", engine="polars")
    assert df.columns == ["timestamp", "channel", "value", "value_str", "tags"]
    assert len(set(df["channel"].to_list())) == 5
    assert sorted(v for v in df["value_str"].to_list() if v is not None) == ["idle", "running"]


def test_readers_yield_chunks_without_pandas() -> None:
    """read_chunks yields Chunk objects holding every sample of the file."""
    chunks = list(nio.read_chunks(DATA / "capture.csv", chunksize=100))
    assert all(isinstance(c, nio.Chunk) for c in chunks)
    assert sum(len(c) for c in chunks) == 406


@pytest.mark.parametrize("fmt", FORMATS)
def test_write_avro_from_a_chunk(tmp_path: Path, fmt: str) -> None:
    """write_avro accepts a Chunk directly and round-trips it."""
    chunk = nio.Chunk.concat(nio.read_chunks(DATA / f"capture.{fmt}")).sort()
    path = instro.write_avro(chunk, tmp_path / "out.avro")
    pd.testing.assert_frame_equal(_sorted(instro.read_avro(path)), _sorted(chunk.to_pandas()))


def test_readers_do_not_import_pandas_or_polars() -> None:
    """Reading an instro file never imports pandas or polars."""
    import subprocess
    import sys

    code = (
        "import sys, nominal.io as n\n"
        f"for _ in n.read_chunks({str(DATA / 'capture.csv')!r}): pass\n"
        "sys.exit(('pandas' in sys.modules) or ('polars' in sys.modules))"
    )
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


def test_missing_fastavro_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without fastavro, reading or writing Avro raises MissingDependencyError pointing at nominal[avro]."""
    import builtins

    real_import = builtins.__import__

    def fake(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "fastavro":
            raise ModuleNotFoundError("No module named 'fastavro'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake)
    with pytest.raises(nio.MissingDependencyError, match=r"nominal\[avro\]"):
        nio.read(DATA / "capture.avro")


def test_jsonl_null_next_to_a_string_is_nan(tmp_path: Path) -> None:
    """In a channel that also holds strings, a JSON null reads as NaN, not None."""
    path = tmp_path / "mixed.jsonl"
    path.write_text('{"channel_data": {"a": ["x", null]}, "timestamps": [1, 2], "tags": null}\n')
    values = instro.read_jsonl(path)["value"].tolist()
    assert values[0] == "x" and values[1] != values[1]  # NaN is the only value that differs from itself


def test_csv_integers_read_as_float(tmp_path: Path) -> None:
    """A CSV chunk of whole numbers still gives a float64 value column."""
    path = tmp_path / "ints.csv"
    path.write_text("timestamp,channel,value,tags\n1,a,1,\n2,a,2,\n")
    assert instro.read_csv(path)["value"].dtype == "float64"
