from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import fastavro
import polars as pl
import pytest

from nominal.thirdparty.polars.polars_avro_writer import (
    _MAX_TIMESTAMP_NS,
    PolarsAvroWriter,
    _encode_object_as_string,
    _series_to_avro_values,
)
from nominal.ts import _SecondsNanos

TS_COL = "ts"


def dt_to_nano(dt: datetime) -> int:
    return _SecondsNanos.from_datetime(dt).to_nanoseconds()


def read_records(path: Path) -> list[dict[str, Any]]:
    with open(path, "rb") as f:
        return list(fastavro.reader(f))


def read_all_records(paths: list[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for p in paths:
        records.extend(read_records(p))
    return records


@pytest.fixture
def ts_start() -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def make_df(ts_start: datetime):
    """Factory for DataFrames with a ``ts`` Int64 column plus the passed-in columns."""

    def _make(n: int = 4, **columns: Any) -> pl.DataFrame:
        timestamps = [dt_to_nano(ts_start + timedelta(seconds=i)) for i in range(n)]
        return pl.DataFrame({TS_COL: pl.Series(timestamps, dtype=pl.Int64), **columns})

    return _make


@pytest.fixture
def writer_path(tmp_path: Path) -> Path:
    return tmp_path / "out.avro"


# =========================================================================
# End-to-end writes
# =========================================================================


def test_round_trip_basic(writer_path: Path, make_df) -> None:
    df = make_df(n=3, temperature=[1.0, 2.0, 3.0], label=["a", "b", "c"])

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("group_a", df)
    files = w.files_written

    assert len(files) == 1
    records = read_records(files[0])

    by_channel = {r["channel"]: r for r in records}
    assert by_channel.keys() == {"temperature", "label"}
    assert by_channel["temperature"]["values"] == [1.0, 2.0, 3.0]
    assert by_channel["label"]["values"] == ["a", "b", "c"]
    assert by_channel["temperature"]["tags"] == {"source_group": "group_a"}


def test_extra_tags_merged(writer_path: Path, make_df) -> None:
    df = make_df(n=2, v=[1.0, 2.0])

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("group_a", df, tags={"vehicle": "truck1"})

    record = read_records(w.files_written[0])[0]
    assert record["tags"] == {"source_group": "group_a", "vehicle": "truck1"}


def test_integer_long_encoding(writer_path: Path, make_df) -> None:
    df = make_df(n=3, count=pl.Series([10, 20, 30], dtype=pl.Int64))

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    record = read_records(w.files_written[0])[0]
    assert record["channel"] == "count"
    assert record["values"] == [10, 20, 30]


def test_integer_double_encoding_with_nulls(writer_path: Path, make_df) -> None:
    df = make_df(n=3, count=pl.Series([10, None, 30], dtype=pl.Int64))

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL, integers_as_double=True) as w:
        w.add("g", df)

    record = read_records(w.files_written[0])[0]
    values = record["values"]
    assert values[0] == 10.0
    assert math.isnan(values[1])
    assert values[2] == 30.0


def test_null_fills_float_with_nan(writer_path: Path, make_df) -> None:
    df = make_df(n=2, v=pl.Series([1.0, None], dtype=pl.Float64))

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    assert values[0] == 1.0
    assert math.isnan(values[1])


def test_null_fills_string_with_empty(writer_path: Path, make_df) -> None:
    df = make_df(n=2, s=pl.Series(["x", None], dtype=pl.Utf8))

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    assert values == ["x", ""]


def test_list_of_floats_becomes_double_array(writer_path: Path, make_df) -> None:
    df = make_df(
        n=2,
        arr=pl.Series([[1.0, 2.0, 3.0], None], dtype=pl.List(pl.Float64)),
    )

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    assert values[0] == {"items": [1.0, 2.0, 3.0]}
    # null outer -> empty items list
    assert values[1] == {"items": []}


def test_list_of_ints_upcasts_to_double_array(writer_path: Path, make_df) -> None:
    df = make_df(n=1, arr=pl.Series([[1, 2, 3]], dtype=pl.List(pl.Int64)))

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    # Schema has no LongArray arm; inners are upcast to float.
    assert values == [{"items": [1.0, 2.0, 3.0]}]


def test_list_of_strings_becomes_string_array(writer_path: Path, make_df) -> None:
    df = make_df(n=2, arr=pl.Series([["a", "b"], None], dtype=pl.List(pl.Utf8)))

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    assert values[0] == {"items": ["a", "b"]}
    assert values[1] == {"items": []}


def test_struct_becomes_json_struct(writer_path: Path, make_df) -> None:
    struct_series = pl.Series(
        [{"x": 1.0, "label": "a"}, {"x": 2.0, "label": "b"}],
        dtype=pl.Struct({"x": pl.Float64, "label": pl.Utf8}),
    )
    df = make_df(n=2, event=struct_series)

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    import json

    assert json.loads(values[0]["json"]) == {"x": 1.0, "label": "a"}
    assert json.loads(values[1]["json"]) == {"x": 2.0, "label": "b"}


def test_struct_null_emits_empty_object(writer_path: Path, make_df) -> None:
    struct_series = pl.Series(
        [{"x": 1.0}, None],
        dtype=pl.Struct({"x": pl.Float64}),
    )
    df = make_df(n=2, event=struct_series)

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    values = read_records(w.files_written[0])[0]["values"]
    assert values[1] == {"json": "{}"}


def test_timestamps_shared_across_channels(writer_path: Path, make_df) -> None:
    df = make_df(n=3, a=[1.0, 2.0, 3.0], b=[4.0, 5.0, 6.0])

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("g", df)

    records = read_records(w.files_written[0])
    assert len(records) == 2
    assert records[0]["timestamps"] == records[1]["timestamps"]


def test_multiple_add_calls(writer_path: Path, make_df) -> None:
    df_a = make_df(n=2, v=[1.0, 2.0])
    df_b = make_df(n=3, v=[10.0, 20.0, 30.0])

    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        w.add("group_a", df_a)
        w.add("group_b", df_b)

    records = read_records(w.files_written[0])
    groups = {r["tags"]["source_group"]: r["values"] for r in records}
    assert groups == {"group_a": [1.0, 2.0], "group_b": [10.0, 20.0, 30.0]}


def test_channel_batching_preserves_all_points(writer_path: Path, make_df) -> None:
    df = make_df(n=10, v=[float(i) for i in range(10)])

    with PolarsAvroWriter(
        writer_path,
        timestamp_column=TS_COL,
        channel_batch_size=3,
    ) as w:
        w.add("g", df)

    records = read_records(w.files_written[0])
    # Four records: 3+3+3+1 rows per batch.
    assert len(records) == 4
    all_values: list[float] = []
    for r in sorted(records, key=lambda r: r["timestamps"][0]):
        all_values.extend(r["values"])
    assert all_values == [float(i) for i in range(10)]


def test_file_rolling(writer_path: Path, make_df) -> None:
    # Force aggressive rolling with a tiny max file size; every few records should roll.
    df = make_df(n=30, v=[float(i) for i in range(30)])

    with PolarsAvroWriter(
        writer_path,
        timestamp_column=TS_COL,
        channel_batch_size=3,
        max_file_bytes=1,
    ) as w:
        w.add("g", df)
    files = w.files_written

    assert len(files) >= 2
    for i, path in enumerate(files):
        assert path.name == f"out_{i:03d}.avro"
        assert path.exists()

    records = read_all_records(files)
    all_values: list[float] = []
    for r in sorted(records, key=lambda r: r["timestamps"][0]):
        all_values.extend(r["values"])
    assert all_values == [float(i) for i in range(30)]


def test_empty_writer_produces_no_files(writer_path: Path) -> None:
    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        pass

    assert w.files_written == []


# =========================================================================
# Error paths
# =========================================================================


def test_missing_timestamp_column_raises(writer_path: Path) -> None:
    df = pl.DataFrame({"other_ts": pl.Series([0], dtype=pl.Int64), "v": [1.0]})
    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        with pytest.raises(ValueError, match="missing timestamp column"):
            w.add("g", df)


def test_wrong_timestamp_dtype_raises(writer_path: Path) -> None:
    df = pl.DataFrame({TS_COL: [1.0, 2.0], "v": [1.0, 2.0]})
    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        with pytest.raises(TypeError, match="expected Int64"):
            w.add("g", df)


def test_negative_timestamp_raises(writer_path: Path) -> None:
    df = pl.DataFrame({TS_COL: pl.Series([-1, 0], dtype=pl.Int64), "v": [1.0, 2.0]})
    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        with pytest.raises(ValueError, match="negative value"):
            w.add("g", df)


def test_timestamp_at_long_max_raises(writer_path: Path) -> None:
    df = pl.DataFrame({TS_COL: pl.Series([_MAX_TIMESTAMP_NS], dtype=pl.Int64), "v": [1.0]})
    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        with pytest.raises(ValueError, match="Long.MAX_VALUE"):
            w.add("g", df)


def test_reserved_tag_key_raises(writer_path: Path, make_df) -> None:
    df = make_df(n=1, v=[1.0])
    with PolarsAvroWriter(writer_path, timestamp_column=TS_COL) as w:
        with pytest.raises(ValueError, match="reserved"):
            w.add("g", df, tags={"nom.ingest_rid": "foo"})


def test_add_after_close_raises(writer_path: Path, make_df) -> None:
    df = make_df(n=1, v=[1.0])
    w = PolarsAvroWriter(writer_path, timestamp_column=TS_COL)
    w.close()
    with pytest.raises(RuntimeError, match="Cannot add after close"):
        w.add("g", df)


# =========================================================================
# Dtype coverage for _series_to_avro_values
# =========================================================================


@pytest.mark.parametrize(
    "series,expected_hint",
    [
        (pl.Series([True, False], dtype=pl.Boolean), "no boolean arm"),
        (
            pl.Series([datetime(2024, 1, 1)], dtype=pl.Datetime("ns")),
            "no datetime arm",
        ),
        (pl.Series([1.0], dtype=pl.Decimal(10, 2)), "no decimal arm"),
    ],
)
def test_rejected_dtypes_raise_with_hint(series: pl.Series, expected_hint: str) -> None:
    with pytest.raises(TypeError, match=expected_hint):
        _series_to_avro_values(series)


def test_integer_long_with_null_raises() -> None:
    series = pl.Series([1, None, 3], dtype=pl.Int64)
    with pytest.raises(TypeError, match="no null representation"):
        _series_to_avro_values(series, integers_as_double=False)


def test_integer_double_with_null_fills_nan() -> None:
    series = pl.Series([1, None, 3], dtype=pl.Int64)
    values = _series_to_avro_values(series, integers_as_double=True)
    assert values[0] == 1.0
    assert math.isnan(values[1])  # type: ignore[arg-type]
    assert values[2] == 3.0


def test_object_column_with_strings_accepted() -> None:
    series = pl.Series(["a", None, "b"], dtype=pl.Object)
    values = _encode_object_as_string(series)
    assert values == ["a", "", "b"]


def test_object_column_with_non_strings_rejected() -> None:
    series = pl.Series([1, 2, 3], dtype=pl.Object)
    with pytest.raises(TypeError, match="non-string values"):
        _encode_object_as_string(series)


def test_struct_with_datetime_inner_rejected() -> None:
    series = pl.Series(
        [{"when": datetime(2024, 1, 1)}],
        dtype=pl.Struct({"when": pl.Datetime("ns")}),
    )
    with pytest.raises(TypeError, match="has no strict JSON mapping"):
        _series_to_avro_values(series)


def test_list_of_boolean_rejected() -> None:
    # Inner-dtype rejection surfaces the generic "not a representable Avro union arm"
    # hint (the per-dtype hint keys on outer type).
    series = pl.Series([[True, False]], dtype=pl.List(pl.Boolean))
    with pytest.raises(TypeError, match="not a representable Avro union arm"):
        _series_to_avro_values(series)
