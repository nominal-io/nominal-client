"""The ``df.nom`` accessor."""

from __future__ import annotations

import datetime
import re
import tempfile
from collections.abc import Mapping
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
from nominal.io.instro import write_avro
from nominal.io.schema import COLUMNS
from nominal.pandas._resolve import resolve

if TYPE_CHECKING:
    import nominal.core


@pd.api.extensions.register_dataframe_accessor("nom")
class NomAccessor:
    """Work with a long-form frame through ``df.nom``.

    Methods that filter return another long-form frame, so they chain::

        df.nom.select("daq.ai_0", "daq.ai_1").nom.between("2025-10-09 08:53:21Z").nom.wide()

    `wide`, `to_avro` and `to_nominal` end a chain: `wide` returns one column per
    channel, for plotting and analysis; the other two write the data out.

    Args:
        df: A DataFrame with the nominal columns (see `nominal.io.schema`).

    Raises:
        AttributeError: If ``df`` is missing any of those columns.
    """

    def __init__(self, df: pd.DataFrame) -> None:  # noqa: D107
        missing = [c for c in COLUMNS if c not in df.columns]
        if missing:
            raise AttributeError(f"df.nom needs columns {COLUMNS}; missing {missing}")
        self._df = df

    @property
    def channels(self) -> list[str]:
        """Channel names, in order of first appearance."""
        return list(self._df["channel"].unique())

    def select(self, *channels: str) -> pd.DataFrame:
        """Keep only the rows of the given channels.

        Args:
            *channels: Channel names to keep.

        Returns:
            A long-form frame.

        Raises:
            KeyError: If a channel is not in the frame.
        """
        unknown = set(channels) - set(self.channels)
        if unknown:
            raise KeyError(f"Unknown channels: {sorted(unknown)}")
        return self._subset(self._df["channel"].isin(channels))

    def between(self, start: Any = None, end: Any = None) -> pd.DataFrame:
        """Keep the rows with ``start <= timestamp < end``.

        Either end can be a duration instead of a time, measured from the other end::

            df.nom.between(t0, "10s")   # the 10 s from t0
            df.nom.between("2s", t1)    # the 2 s before t1

        Args:
            start: Earliest time to keep, or ``None`` for no lower bound. Anything
                `pandas.Timestamp` accepts; a time without a zone is taken as UTC. Or a
                duration before ``end``.
            end: Time to stop before, or ``None`` for no upper bound. Or a duration after
                ``start``.

        Returns:
            A long-form frame.

        Raises:
            ValueError: If a duration is given without a time at the other end.

        A duration is a `pandas.Timedelta` or `datetime.timedelta`, or a string of numbers
        and units such as ``"10s"``, ``"2ms"`` or ``"1min 30s"``.
        """
        start_duration, end_duration = _duration(start), _duration(end)
        if start_duration is not None and end_duration is not None:
            raise ValueError("between needs a time at one end; both start and end are durations")
        if end_duration is not None:
            if start is None:
                raise ValueError("between needs a start time when end is a duration")
            start = _utc(start)
            end = start + end_duration
        elif start_duration is not None:
            if end is None:
                raise ValueError("between needs an end time when start is a duration")
            end = _utc(end)
            start = end - start_duration
        ts = self._df["timestamp"]
        mask = pd.Series(True, index=self._df.index)
        if start is not None:
            mask &= ts >= _utc(start)
        if end is not None:
            mask &= ts < _utc(end)
        return self._subset(mask)

    def first(self, duration: Any) -> pd.DataFrame:
        """Keep the rows within ``duration`` of the frame's first timestamp.

        Args:
            duration: How long, e.g. ``"10s"`` or ``"2ms"``; see `between`.

        Returns:
            A long-form frame: rows with ``first <= timestamp < first + duration``.
        """
        if self._df.empty:
            return self._subset(pd.Series(True, index=self._df.index))
        return self.between(self._df["timestamp"].min(), _required_duration(duration))

    def last(self, duration: Any) -> pd.DataFrame:
        """Keep the rows within ``duration`` of the frame's last timestamp.

        Args:
            duration: How long, e.g. ``"1min"``; see `between`.

        Returns:
            A long-form frame: rows with ``last - duration <= timestamp <= last``.
        """
        if self._df.empty:
            return self._subset(pd.Series(True, index=self._df.index))
        ts = self._df["timestamp"]
        return self._subset(ts >= ts.max() - _required_duration(duration))

    def with_tags(self, **tags: str) -> pd.DataFrame:
        """Keep the rows whose tags include every given key and value.

        Args:
            **tags: Tag names and the values they must have, e.g. ``run="steady"``.

        Returns:
            A long-form frame.
        """
        items = tags.items()
        return self._subset(self._df["tags"].map(lambda t: items <= t.items()).astype(bool))

    def wide(self) -> pd.DataFrame:
        """Pivot to one column per channel, indexed by timestamp.

        Channels sampled at different times leave ``NaN`` where a channel has no
        sample. If a channel has several rows at one timestamp, the last is kept.
        Tags are dropped.

        Returns:
            A `~pandas.DataFrame` with a ``timestamp`` index and one column per channel;
            numeric channels are ``float64``. This is not a long-form frame.
        """
        df = self._df.drop_duplicates(["timestamp", "channel"], keep="last")
        return df.pivot(index="timestamp", columns="channel", values="value").infer_objects()

    def to_avro(self, path: str | PathLike[str]) -> Path:
        """Write to an Avro file in Nominal's ``AvroStream`` schema.

        This is the format `~instro.lib.publishers.FilePublisher` writes with ``format="avro"``,
        so `nominal.io.instro.read_avro` reads it back and `nominal.core.Dataset.add_avro_stream` uploads it. Rows with
        the same channel and tags are written together, in time order, so rows that
        share a timestamp may come back in a different order. Missing values (``None``,
        ``NaN``) are written as ``NaN``.

        Args:
            path: Where to write the file; should end in ``.avro``.

        Returns:
            ``path``, as a `pathlib.Path`.
        """
        return write_avro(self._df, path)

    def to_nominal(
        self,
        *,
        asset: nominal.core.Asset | str | None = None,
        dataset: nominal.core.Dataset | str | None = None,
        profile: str | None = None,
        client: nominal.core.NominalClient | None = None,
        tags: Mapping[str, str] | None = None,
        wait: bool = False,
    ) -> tuple[nominal.core.Asset, nominal.core.Dataset, nominal.core.DatasetFile]:
        """Upload to Nominal, finding or creating the asset and dataset to put it in.

        With no arguments, the data goes in a dataset called ``"nominal"`` on an asset
        called ``"nominal-pandas"`` (`~nominal.pandas.DEFAULT`), both created the first time::

            asset, dataset, file = df.nom.to_nominal()
            asset, dataset, file = df.nom.to_nominal(asset="ECG record 119", dataset="lead II")
            asset, dataset, file = df.nom.to_nominal(profile="staging")

        Writes a temporary Avro file with `to_avro` and uploads it with
        `~nominal.core.Dataset.add_avro_stream`. Nominal then ingests it, which takes a
        moment longer; pass ``wait=True`` to return only once it has.

        Args:
            asset: An `nominal.core.Asset`, or the name of one: the asset with exactly that
                name, created if there is none. Defaults to ``"nominal-pandas"``.
            dataset: A `nominal.core.Dataset`, added to the asset if it isn't on it already
                (under its name, or its RID if another of the asset's datasets has that name);
                or the name of the asset's dataset, created if there is none. Defaults to
                ``"nominal"``.
            profile: The profile to upload with (`~nominal.config.list_profiles`). Defaults to the
                one ``asset`` or ``dataset`` came from, or else the active profile
                (`~nominal.config.active_profile`).
            client: The `nominal.core.NominalClient` to use, instead of a ``profile``.
            tags: Tags for every row, e.g. ``{"test_id": "42"}``. A row's own tags
                win where keys overlap.
            wait: Wait until Nominal has ingested the data
                (`~nominal.core.DatasetFile.poll_until_ingestion_completed`).

        Returns:
            The asset, the dataset, and the uploaded `nominal.core.DatasetFile`.

        Raises:
            ValueError: If ``asset`` is a name that several assets share, ``profile`` doesn't
                exist, both ``profile`` and ``client`` are given, or one of them is given with an
                `~nominal.core.Asset` (which already has its own connection).
            nominal.io.MissingDependencyError: If ``fastavro`` isn't installed (``pip install nominal[avro]``).
            nominal.exceptions.NominalIngestError: If ``wait`` is set and ingestion fails.
        """
        with tempfile.TemporaryDirectory() as tmp:
            avro = self.to_avro(Path(tmp) / "nominal.avro")  # first, so a missing fastavro creates nothing in Nominal
            asset, dataset = resolve(asset, dataset, client, profile)
            file = dataset.add_avro_stream(avro, tags=tags)
        return asset, dataset, file.poll_until_ingestion_completed() if wait else file

    def _subset(self, mask: pd.Series[bool]) -> pd.DataFrame:
        """Return the rows in ``mask``, re-typing ``value`` as ``float64`` if only numbers remain."""
        df = self._df[mask].reset_index(drop=True)
        if df["value"].dtype == object:
            df["value"] = _values(df["value"].tolist())
        return df


def _utc(t: Any) -> pd.Timestamp:
    """Convert ``t`` to a UTC `pandas.Timestamp`, treating a zoneless time as UTC."""
    ts = pd.Timestamp(t)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _values(values: Any) -> pd.Series[Any]:
    """Return ``values`` as ``float64`` if all are numbers or ``None``, else as ``object``."""
    if all(v is None or (isinstance(v, (int, float)) and not isinstance(v, bool)) for v in values):
        return pd.Series(values, dtype="float64")
    return pd.Series(values, dtype=object)


_DURATION = re.compile(r"\s*(\d+(\.\d*)?|\.\d+)\s*[a-zA-Zµ]+(\s*(\d+(\.\d*)?|\.\d+)\s*[a-zA-Zµ]+)*\s*")


def _duration(value: Any) -> pd.Timedelta | None:
    """Return ``value`` as a `pandas.Timedelta` if it's a duration, else ``None`` (it's a time, or ``None``).

    Strings count only as numbers with units (``"10s"``, ``"1min 30s"``): pandas would also
    read ``"09:00:00"`` as nine hours, but that's more likely meant as a time of day.
    """
    if isinstance(value, (pd.Timedelta, datetime.timedelta)):
        return pd.Timedelta(value)
    if isinstance(value, str) and _DURATION.fullmatch(value):
        return pd.Timedelta(value)
    return None


def _required_duration(value: Any) -> pd.Timedelta:
    """Return ``value`` as a `pandas.Timedelta`, or raise if it isn't a duration."""
    duration = _duration(value)
    if duration is None:
        raise ValueError(f"expected a duration such as '10s', got {value!r}")
    return duration
