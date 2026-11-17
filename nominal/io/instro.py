"""Read and write files from instro's ``FilePublisher``: CSV, Avro and JSON Lines.

Registers the formats ``instro-csv``, ``instro-avro`` and ``instro-jsonl``. Avro needs ``fastavro``
(``pip install nominal[avro]``).
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Iterator
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import numpy as np

from nominal.io._reader import MissingDependencyError, Reader, ReaderSpec
from nominal.io.schema import COLUMNS, Chunk, ChunkBuilder

if TYPE_CHECKING:
    import pandas as pd

CSV_SPEC = ReaderSpec(
    "instro-csv",
    (".csv",),
    "nominal.io.instro:CsvReader",
    "instro FilePublisher CSV (timestamp, channel, value, tags)",
)
AVRO_SPEC = ReaderSpec(
    "instro-avro",
    (".avro",),
    "nominal.io.instro:AvroReader",
    "instro FilePublisher Avro (Nominal's AvroStream schema)",
    "pip install nominal[avro]",
)
JSONL_SPEC = ReaderSpec(
    "instro-jsonl",
    (".jsonl",),
    "nominal.io.instro:JsonlReader",
    "instro FilePublisher JSON Lines (Measurements and Commands)",
)

_CSV_HEADER = ",".join(COLUMNS)
_AVRO_RECORD_SIZE = 10_000  # samples per record; keeps records small for big channels
_AVRO_SCHEMA_DEF: dict[str, Any] = {
    "type": "record",
    "namespace": "io.nominal.ingest",
    "name": "AvroStream",
    "fields": [
        {"name": "channel", "type": "string"},
        {"name": "timestamps", "type": {"type": "array", "items": "long"}},
        {"name": "values", "type": {"type": "array", "items": ["double", "string"]}},
        {"name": "tags", "type": {"type": "map", "values": "string"}, "default": {}},
    ],
}


def _fastavro() -> Any:
    """Import fastavro, or say how to install it."""
    try:
        import fastavro
    except ImportError as e:
        raise MissingDependencyError(f"Avro needs fastavro: {AVRO_SPEC.install_hint}") from e
    return fastavro


class CsvReader(Reader):
    """CSV written by ``FilePublisher(format="csv")``.

    CSV stores every value as text, so a value is read as a number when it parses as one and kept as
    a string otherwise. A string channel whose values look numeric (``"1"``) therefore comes back as
    numbers; use Avro to avoid this.
    """

    def sniff(self, path: str | PathLike[str]) -> bool | None:
        """Return whether the file's header row is ``timestamp,channel,value,tags``."""
        try:
            with open(path, encoding="utf-8") as f:
                return f.readline().strip() == _CSV_HEADER
        except (OSError, UnicodeDecodeError):
            return False

    def read_chunks(self, path: str | PathLike[str], *, chunksize: int = 100_000, **options: Any) -> Iterator[Chunk]:
        """Yield the file ``chunksize`` rows at a time."""
        tag_cache: dict[str, dict[str, str]] = {}
        with open(path, newline="", encoding="utf-8") as f:
            rows = csv.reader(f)
            header = next(rows, None)
            if header is None:
                return
            index = {name: header.index(name) for name in COLUMNS}
            timestamps: list[int] = []
            channels: list[str] = []
            values: list[Any] = []
            tags: list[dict[str, str]] = []
            for row in rows:
                timestamps.append(int(row[index["timestamp"]]))
                channels.append(row[index["channel"]])
                values.append(_csv_value(row[index["value"]]))
                text = row[index["tags"]]
                if text not in tag_cache:
                    tag_cache[text] = json.loads(text) if text else {}
                tags.append(tag_cache[text])
                if len(timestamps) >= chunksize:
                    yield _rows_chunk(timestamps, channels, values, tags)
                    timestamps, channels, values, tags = [], [], [], []
            if timestamps:
                yield _rows_chunk(timestamps, channels, values, tags)


class AvroReader(Reader):
    """Avro written by ``FilePublisher(format="avro")``, in Nominal's ``AvroStream`` schema.

    One record per channel per publish, holding parallel ``timestamps`` and ``values`` arrays.
    """

    def sniff(self, path: str | PathLike[str]) -> bool | None:
        """Return whether the file starts with the Avro magic bytes."""
        try:
            with open(path, "rb") as f:
                return f.read(4) == b"Obj\x01"
        except OSError:
            return False

    def read_chunks(self, path: str | PathLike[str], *, chunksize: int = 100_000, **options: Any) -> Iterator[Chunk]:
        """Yield the file about ``chunksize`` samples at a time, cut on record boundaries."""
        fastavro = _fastavro()
        builder = ChunkBuilder()
        with open(path, "rb") as f:
            for record in cast(Iterable[dict[str, Any]], fastavro.reader(f)):
                builder.add(record["timestamps"], record["channel"], record["values"], record["tags"])
                if len(builder) >= chunksize:
                    yield builder.build()
                    builder = ChunkBuilder()
        if len(builder):
            yield builder.build()


class JsonlReader(Reader):
    """JSON Lines written by ``FilePublisher(format="jsonl")``.

    Each line is a Measurement (a ``timestamps`` list and a list of values per channel) or a Command
    (one ``timestamp`` and one value per channel). Non-finite floats are written as ``null`` and
    read back as ``NaN``.
    """

    def sniff(self, path: str | PathLike[str]) -> bool | None:
        """Return whether the first line is a JSON object with ``channel_data``."""
        try:
            with open(path, encoding="utf-8") as f:
                first = f.readline()
            return "channel_data" in json.loads(first)
        except (OSError, ValueError, TypeError):
            return False

    def read_chunks(self, path: str | PathLike[str], *, chunksize: int = 100_000, **options: Any) -> Iterator[Chunk]:
        """Yield the file about ``chunksize`` samples at a time."""
        builder = ChunkBuilder()
        with open(path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                record = json.loads(line)
                tags = record.get("tags") or {}
                if "timestamps" in record:  # Measurement
                    for channel, samples in record["channel_data"].items():
                        builder.add(record["timestamps"], channel, samples, tags)
                else:  # Command
                    for channel, sample in record["channel_data"].items():
                        builder.add([record["timestamp"]], channel, [sample], tags)
                if len(builder) >= chunksize:
                    yield builder.build()
                    builder = ChunkBuilder()
        if len(builder):
            yield builder.build()


def read_csv(path: str | PathLike[str], *, engine: str = "pandas") -> Any:
    """Read an instro CSV file as one frame (``engine`` is ``"pandas"`` or ``"polars"``); see `CsvReader`."""
    return Chunk.concat(CsvReader().read_chunks(path)).to_frame(engine)


def read_avro(path: str | PathLike[str], *, engine: str = "pandas") -> Any:
    """Read an instro Avro file as one frame (``engine`` is ``"pandas"`` or ``"polars"``); see `AvroReader`."""
    return Chunk.concat(AvroReader().read_chunks(path)).to_frame(engine)


def read_jsonl(path: str | PathLike[str], *, engine: str = "pandas") -> Any:
    """Read an instro JSON Lines file as one frame (``engine`` is ``"pandas"`` or ``"polars"``); see `JsonlReader`."""
    return Chunk.concat(JsonlReader().read_chunks(path)).to_frame(engine)


def write_avro(data: Chunk | pd.DataFrame, path: str | PathLike[str]) -> Path:
    """Write samples as Avro in Nominal's ``AvroStream`` schema, which Nominal can ingest.

    One record per channel and tag set, in time order, split into records of at most 10,000 samples.
    Missing values (``None``, ``NaN``) are written as ``NaN``. Needs ``fastavro`` (``pip install nominal[avro]``).

    Args:
        data: A `~nominal.io.schema.Chunk`, or a long-form pandas frame (see `nominal.io.schema`).
        path: Where to write it.

    Returns:
        ``path``, as a `~pathlib.Path`.
    """
    fastavro = _fastavro()
    chunk = data if isinstance(data, Chunk) else Chunk.from_pandas(data)
    path = Path(path)
    with open(path, "wb") as f:
        fastavro.writer(f, fastavro.parse_schema(_AVRO_SCHEMA_DEF), _avro_records(chunk.validate()), codec="snappy")
    return path


def _csv_value(text: str) -> Any:
    """Return a CSV cell as a float if it parses as one (an empty cell is missing), else as the string."""
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return text


def _rows_chunk(timestamps: list[int], channels: list[str], values: list[Any], tags: list[dict[str, str]]) -> Chunk:
    builder = ChunkBuilder()
    builder.add_rows(timestamps, channels, values, tags)
    return builder.build()


def _avro_records(chunk: Chunk) -> Iterator[dict[str, Any]]:
    """Yield ``AvroStream`` records, one per channel and tag set, in time order, split into chunks."""
    chunk = chunk.sort()
    if not len(chunk):
        return
    key = chunk.channels.astype(np.int64) * len(chunk.tag_sets) + chunk.tags
    order = np.argsort(key, kind="stable")  # stable: time order is kept within each channel and tag set
    for rows in np.split(order, np.flatnonzero(np.diff(key[order])) + 1):
        channel = chunk.channel_names[int(chunk.channels[rows[0]])]
        tags = dict(chunk.tag_sets[int(chunk.tags[rows[0]])])
        timestamps = chunk.timestamps_ns[rows].tolist()
        values: list[Any] = chunk.values[rows].tolist()
        if chunk.strings is not None:
            values = [s if s is not None else v for s, v in zip(chunk.strings[rows].tolist(), values)]
        for i in range(0, len(timestamps), _AVRO_RECORD_SIZE):
            yield {
                "channel": channel,
                "timestamps": timestamps[i : i + _AVRO_RECORD_SIZE],
                "values": values[i : i + _AVRO_RECORD_SIZE],
                "tags": tags,
            }
