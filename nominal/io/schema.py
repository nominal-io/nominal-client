"""The data every reader produces, independent of any DataFrame library.

A reader yields `Chunk` objects: columns of plain numpy arrays, one row per sample. `nominal.io`
turns chunks into a pandas or polars frame only at the edge (`Chunk.to_frame`), so no reader depends
on either library.

A frame has these columns (pandas layout; see `Chunk.to_polars` for polars):

========== =========================== ==========================================
column     dtype                       contents
========== =========================== ==========================================
timestamp  ``datetime64[ns, UTC]``     sample time
channel    ``object`` (str)            channel name, e.g. ``"daq.ai_0"``
value      ``float64`` or ``object``   ``float64`` when every value is numeric,
                                       otherwise floats and strings mixed
tags       ``object`` (dict)           tag dict, ``{}`` when untagged
========== =========================== ==========================================

Rows are sorted by timestamp (stably, so rows sharing a timestamp keep file order).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

COLUMNS = ["timestamp", "channel", "value", "tags"]
"""Columns of a long-form pandas frame, in order."""

ENGINES = ("pandas", "polars")
"""The frame libraries a `Chunk` converts to."""


@dataclass(frozen=True, eq=False)
class Chunk:
    """A batch of samples as columns of numpy arrays: what a `~nominal.io.Reader` yields.

    Channel names and tag sets are stored once and referred to by integer codes, because a file
    has far fewer distinct channels and tag sets than samples.

    Build one with `ChunkBuilder`. Rows needn't be sorted; `sort` and `Chunk.concat` do that.

    Args:
        timestamps_ns: Nanoseconds since the Unix epoch, ``int64``, one per sample.
        channels: Index into ``channel_names`` for each sample.
        channel_names: The distinct channel names.
        values: ``float64`` value of each sample; ``NaN`` where the sample is a string or missing.
        strings: ``None`` if every sample is numeric; otherwise an ``object`` array holding the
            string of each string sample and ``None`` elsewhere.
        tags: Index into ``tag_sets`` for each sample.
        tag_sets: The distinct tag dicts, e.g. ``({}, {"run": "steady"})``.
    """

    timestamps_ns: np.ndarray[Any, Any]
    channels: np.ndarray[Any, Any]
    channel_names: tuple[str, ...]
    values: np.ndarray[Any, Any]
    strings: np.ndarray[Any, Any] | None
    tags: np.ndarray[Any, Any]
    tag_sets: tuple[Mapping[str, str], ...]

    def __len__(self) -> int:  # noqa: D105
        return len(self.timestamps_ns)

    def validate(self) -> Chunk:
        """Return ``self`` if the arrays agree with each other, else raise.

        Raises:
            ValueError: If lengths or dtypes differ, or a code points outside its name or tag list.
        """
        n = len(self.timestamps_ns)
        columns = {"channels": self.channels, "values": self.values, "tags": self.tags}
        if self.strings is not None:
            columns["strings"] = self.strings
        for name, column in columns.items():
            if len(column) != n:
                raise ValueError(f"Chunk.{name} has {len(column)} rows but timestamps_ns has {n}")
        if self.timestamps_ns.dtype != np.int64:
            raise ValueError(f"Chunk.timestamps_ns must be int64, got {self.timestamps_ns.dtype}")
        if self.values.dtype != np.float64:
            raise ValueError(f"Chunk.values must be float64, got {self.values.dtype}")
        for name, codes, size in (
            ("channels", self.channels, len(self.channel_names)),
            ("tags", self.tags, len(self.tag_sets)),
        ):
            if n and (codes.min() < 0 or codes.max() >= size):
                raise ValueError(f"Chunk.{name} has a code outside 0..{size - 1}")
        return self

    def sort(self) -> Chunk:
        """Return the chunk sorted by timestamp (stably, so ties keep their order)."""
        ts = self.timestamps_ns
        if len(ts) < 2 or bool(np.all(ts[1:] >= ts[:-1])):
            return self
        return self._take(np.argsort(ts, kind="stable"))

    @staticmethod
    def concat(chunks: Iterable[Chunk]) -> Chunk:
        """Join chunks into one, merging their channel names and tag sets. Doesn't sort."""
        parts = list(chunks)
        if not parts:
            return ChunkBuilder().build()
        if len(parts) == 1:
            return parts[0]
        names: dict[str, int] = {}
        tag_keys: dict[str, int] = {}
        tag_sets: list[Mapping[str, str]] = []
        channel_codes, tag_codes = [], []
        for part in parts:
            c_map = np.array([names.setdefault(n, len(names)) for n in part.channel_names], dtype=np.intp)
            t_map = []
            for tag_set in part.tag_sets:
                key = _tag_key(tag_set)
                if key not in tag_keys:
                    tag_keys[key] = len(tag_sets)
                    tag_sets.append(tag_set)
                t_map.append(tag_keys[key])
            channel_codes.append(c_map[part.channels] if len(part) else part.channels.astype(np.intp))
            tag_codes.append(np.array(t_map, dtype=np.intp)[part.tags] if len(part) else part.tags.astype(np.intp))
        strings = None
        if any(p.strings is not None for p in parts):
            strings = np.concatenate([p.strings if p.strings is not None else np.full(len(p), None) for p in parts])
        return Chunk(
            np.concatenate([p.timestamps_ns for p in parts]),
            np.concatenate(channel_codes),
            tuple(names),
            np.concatenate([p.values for p in parts]),
            strings,
            np.concatenate(tag_codes),
            tuple(tag_sets),
        )

    def to_frame(self, engine: str = "pandas") -> Any:
        """Return the chunk, sorted by time, as a ``pandas`` or ``polars`` DataFrame.

        Args:
            engine: ``"pandas"`` or ``"polars"``.

        Raises:
            ValueError: If ``engine`` is neither.
        """
        if engine == "pandas":
            return self.to_pandas()
        if engine == "polars":
            return self.to_polars()
        raise ValueError(f"unknown engine {engine!r}; expected one of {', '.join(ENGINES)}")

    def to_pandas(self) -> pd.DataFrame:
        """Return the chunk, sorted by time, as a long-form pandas DataFrame with the `COLUMNS`."""
        import pandas as pd

        chunk = self.sort()
        if chunk.strings is None:
            value: pd.Series[Any] = pd.Series(chunk.values, dtype="float64")
        else:
            mixed = np.where(chunk.strings != None, chunk.strings, chunk.values.astype(object))  # noqa: E711
            value = pd.Series(mixed, dtype=object)
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(chunk.timestamps_ns, unit="ns", utc=True),
                "channel": pd.Series(_pick(chunk.channel_names, chunk.channels), dtype=object),
                "value": value,
                "tags": pd.Series(_pick(chunk.tag_sets, chunk.tags), dtype=object),
            }
        )

    def to_polars(self) -> pl.DataFrame:
        """Return the chunk, sorted by time, as a polars DataFrame.

        .. warning:: Experimental: this layout may change.

        Columns: ``timestamp`` (``Datetime("ns", "UTC")``), ``channel`` (``String``), ``value``
        (``Float64``; null for string samples), ``value_str`` (``String``; null for numeric samples) and
        ``tags`` (a list of ``{key, value}`` structs).
        """
        import polars as pl

        chunk = self.sort()
        strings = chunk.strings if chunk.strings is not None else np.full(len(chunk), None)
        tag_lists = [[{"key": k, "value": v} for k, v in tag_set.items()] for tag_set in chunk.tag_sets]
        tag_type = pl.List(pl.Struct({"key": pl.String, "value": pl.String}))
        is_string = strings != None  # noqa: E711
        return pl.DataFrame(
            {
                "timestamp": pl.Series(chunk.timestamps_ns).cast(pl.Datetime("ns", "UTC")),
                "channel": pl.Series(_pick(chunk.channel_names, chunk.channels).tolist(), dtype=pl.String),
                "value": pl.Series(np.where(is_string, np.nan, chunk.values), nan_to_null=True),
                "value_str": pl.Series(strings.tolist(), dtype=pl.String),
                "tags": pl.Series(tag_lists, dtype=tag_type)[chunk.tags],
            }
        )

    @staticmethod
    def from_pandas(df: pd.DataFrame) -> Chunk:
        """Convert a long-form pandas frame (see `validate_frame`) to a chunk."""
        validate_frame(df)
        builder = ChunkBuilder()
        builder.add_rows(
            df["timestamp"].to_numpy(dtype="datetime64[ns]").view("int64"),
            df["channel"].tolist(),
            df["value"].tolist(),
            df["tags"].tolist(),
        )
        return builder.build()

    def _take(self, order: np.ndarray[Any, Any]) -> Chunk:
        return Chunk(
            self.timestamps_ns[order],
            self.channels[order],
            self.channel_names,
            self.values[order],
            None if self.strings is None else self.strings[order],
            self.tags[order],
            self.tag_sets,
        )


class ChunkBuilder:
    """Collect samples, then `build` a `Chunk`.

    ::

        b = ChunkBuilder()
        b.add(times_ns, "daq.ai_0", values, {"run": "steady"})   # many samples of one channel and tag set
        b.add_rows([t0], ["daq.state"], ["idle"])                 # rows of any channels
        chunk = b.build()
    """

    def __init__(self) -> None:  # noqa: D107
        self._names: dict[str, int] = {}
        self._tag_keys: dict[str, int] = {}
        self._tag_sets: list[Mapping[str, str]] = []
        self._blocks: list[tuple[np.ndarray[Any, Any], np.ndarray[Any, Any], Any, np.ndarray[Any, Any]]] = []
        self._rows = 0

    def __len__(self) -> int:  # noqa: D105
        return self._rows

    def add(
        self,
        timestamps_ns: Sequence[int] | np.ndarray[Any, Any],
        channel: str,
        values: Sequence[Any] | np.ndarray[Any, Any],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add samples of one channel that share a tag set.

        Args:
            timestamps_ns: Integer nanoseconds since the Unix epoch.
            channel: The channel's name.
            values: One per timestamp: numbers, ``None`` (missing) or strings. A float array is fastest.
            tags: The tag dict, or ``None`` for none.
        """
        ts = np.asarray(timestamps_ns, dtype=np.int64)
        self._append(
            ts, np.full(len(ts), self._channel(channel), dtype=np.intp), values, self._tag_codes(tags, len(ts))
        )

    def add_rows(
        self,
        timestamps_ns: Sequence[int] | np.ndarray[Any, Any],
        channels: Sequence[str],
        values: Sequence[Any] | np.ndarray[Any, Any],
        tags: Mapping[str, str] | Sequence[Mapping[str, str]] | None = None,
    ) -> None:
        """Add samples of any channels.

        Args:
            timestamps_ns: Integer nanoseconds since the Unix epoch.
            channels: One channel name per sample.
            values: One per sample: numbers, ``None`` (missing) or strings.
            tags: One tag dict for all samples, one per sample, or ``None`` for none.
        """
        ts = np.asarray(timestamps_ns, dtype=np.int64)
        codes = np.fromiter((self._channel(c) for c in channels), dtype=np.intp, count=len(ts))
        if tags is None or isinstance(tags, Mapping):
            tag_codes = self._tag_codes(tags, len(ts))
        else:
            tag_codes = np.fromiter((self._tag_code(t) for t in tags), dtype=np.intp, count=len(ts))
        self._append(ts, codes, values, tag_codes)

    def build(self) -> Chunk:
        """Return the collected samples as a `Chunk`."""
        if not self._blocks:
            empty = np.empty(0, dtype=np.intp)
            return Chunk(np.empty(0, np.int64), empty, (), np.empty(0, np.float64), None, empty, ())
        floats, strings, any_string = [], [], False
        for ts, _, values, _ in self._blocks:
            f, s = _split_values(values, len(ts))
            floats.append(f)
            strings.append(s)
            any_string = any_string or s is not None
        return Chunk(
            np.concatenate([b[0] for b in self._blocks]),
            np.concatenate([b[1] for b in self._blocks]),
            tuple(self._names),
            np.concatenate(floats),
            np.concatenate([s if s is not None else np.full(len(f), None) for f, s in zip(floats, strings)])
            if any_string
            else None,
            np.concatenate([b[3] for b in self._blocks]),
            tuple(self._tag_sets),
        ).validate()

    def _append(self, ts: Any, channels: Any, values: Any, tags: Any) -> None:
        if len(values) != len(ts):
            raise ValueError(f"{len(values)} values for {len(ts)} timestamps")
        self._blocks.append((ts, channels, values, tags))
        self._rows += len(ts)

    def _channel(self, name: str) -> int:
        return self._names.setdefault(name, len(self._names))

    def _tag_code(self, tags: Mapping[str, str] | None) -> int:
        tags = tags or {}
        key = _tag_key(tags)
        if key not in self._tag_keys:
            self._tag_keys[key] = len(self._tag_sets)
            self._tag_sets.append(dict(tags))
        return self._tag_keys[key]

    def _tag_codes(self, tags: Mapping[str, str] | None, n: int) -> np.ndarray[Any, Any]:
        return np.full(n, self._tag_code(tags), dtype=np.intp)


def validate_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``df`` if it is a long-form pandas frame, else raise.

    Checks the columns and the ``timestamp`` dtype, not the contents of every row.

    Raises:
        ValueError: If the columns aren't `COLUMNS` in order, or ``timestamp`` isn't UTC datetimes.
    """
    import pandas as pd

    if list(df.columns) != COLUMNS:
        raise ValueError(f"expected columns {COLUMNS}, got {list(df.columns)}")
    dtype = df["timestamp"].dtype
    if not isinstance(dtype, pd.DatetimeTZDtype) or str(dtype.tz) != "UTC":
        raise ValueError(f"'timestamp' must be timezone-aware UTC datetimes, got {dtype}")
    return df


def _tag_key(tags: Mapping[str, str] | None) -> str:
    return json.dumps(tags or {}, sort_keys=True)


def _pick(items: Sequence[Any], codes: np.ndarray[Any, Any]) -> np.ndarray[Any, Any]:
    """Return an object array of ``items[code]`` for each code."""
    pool = np.empty(len(items), dtype=object)
    for i, item in enumerate(items):
        pool[i] = item
    picked: np.ndarray[Any, Any] = pool[codes]
    return picked


def _split_values(values: Any, n: int) -> tuple[np.ndarray[Any, Any], np.ndarray[Any, Any] | None]:
    """Split values into a float64 array and, if any are strings, an object array of them."""
    if isinstance(values, np.ndarray) and values.dtype.kind in "fiu":
        return values.astype(np.float64, copy=False), None
    floats = np.full(n, np.nan, dtype=np.float64)
    strings: np.ndarray[Any, Any] | None = None
    for i, v in enumerate(values):
        if isinstance(v, str):
            if strings is None:
                strings = np.full(n, None, dtype=object)
            strings[i] = v
        elif isinstance(v, bool):
            raise TypeError("boolean values aren't supported; convert them to numbers or strings")
        elif v is not None:
            floats[i] = v
    return floats, strings
