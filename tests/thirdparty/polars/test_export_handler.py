from __future__ import annotations

import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from nominal_api import scout_dataexport_api

from nominal.core.channel import Channel, ChannelDataType
from nominal.core.datasource import DataSource
from nominal.thirdparty.polars.export_handler import (
    ExportHandler,
    _build_channel_groups,
    _ExportJob,
    _TimeRange,
)


def _ch(name: str, *, data_source: str = "ds1", data_type: ChannelDataType = ChannelDataType.DOUBLE) -> Channel:
    """A lightweight channel stub exposing only the attributes planning reads."""
    return cast(Channel, SimpleNamespace(name=name, data_source=data_source, data_type=data_type))


def _handler(
    *,
    points_per_request: int = 1_000_000_000,
    points_per_dataframe: int = 1_000_000_000,
    channels_per_request: int = 1_000,
    compression: scout_dataexport_api.CompressionFormat | None = scout_dataexport_api.CompressionFormat.GZIP,
) -> ExportHandler:
    return ExportHandler(
        MagicMock(),
        points_per_request=points_per_request,
        points_per_dataframe=points_per_dataframe,
        channels_per_request=channels_per_request,
        num_workers=1,
        compression=compression,
    )


# ---- _build_channel_groups ----


def test_build_channel_groups_respects_channels_per_request() -> None:
    """With rate non-binding, groups are capped purely by channels_per_request."""
    channels = {f"c{i}": _ch(f"c{i}") for i in range(5)}
    rates = {name: 1.0 for name in channels}

    groups, large = _build_channel_groups(
        rates,
        channels,
        points_per_request=1_000_000,
        channels_per_request=2,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert [len(g) for g in groups] == [2, 2, 1]
    assert large == []


def test_build_channel_groups_splits_on_rate_and_flags_large_channels() -> None:
    """Per-group point rate caps grouping, and a channel exceeding the budget alone is 'large'."""
    channels = {"a": _ch("a"), "b": _ch("b"), "huge": _ch("huge")}
    rates = {"a": 60.0, "b": 60.0, "huge": 150.0}

    # allowed_rate_per_group = points_per_request / batch_seconds = 100 / 1 = 100
    groups, large = _build_channel_groups(
        rates, channels, points_per_request=100, channels_per_request=100, batch_duration=datetime.timedelta(seconds=1)
    )

    # a (60) and b (60) cannot share a 100-rate group; huge (150) exceeds the budget alone.
    assert sorted(len(g) for g in groups) == [1, 1]
    assert [c.name for c in large] == ["huge"]


# ---- _compute_batch_duration ----


def test_compute_batch_duration_from_point_rate() -> None:
    """Batch duration is points_per_dataframe / total_rate, truncated to the range duration."""
    handler = _handler(points_per_dataframe=1000)
    time_range = _TimeRange(0, 100 * 1_000_000_000)  # 100s

    duration_ns = handler._compute_batch_duration(None, [], time_range, {"a": 100.0})

    # 1000 points / 100 pps = 10s, which is < the 100s range
    assert duration_ns == 10 * 1_000_000_000


def test_compute_batch_duration_explicit_passthrough() -> None:
    """An explicit batch_duration is used verbatim (converted to ns)."""
    handler = _handler()
    duration_ns = handler._compute_batch_duration(
        datetime.timedelta(seconds=3), [], _TimeRange(0, 100 * 1_000_000_000), {"a": 100.0}
    )
    assert duration_ns == 3 * 1_000_000_000


def test_compute_batch_duration_zero_rate_uses_full_range() -> None:
    """With no detected data rate, the whole range is exported in a single batch."""
    handler = _handler()
    time_range = _TimeRange(0, 42 * 1_000_000_000)
    assert handler._compute_batch_duration(None, [], time_range, {}) == time_range.duration_ns()


# ---- _compute_export_jobs ----


def test_compute_export_jobs_single_group_propagates_compression() -> None:
    """A small export becomes one slice / one job, tagged with the handler's compression and timestamp type."""
    handler = _handler(compression=None)
    channels = [_ch("a"), _ch("b")]

    with patch.object(handler, "_compute_channel_points_per_second", return_value={"a": 10.0, "b": 5.0}):
        jobs = handler._compute_export_jobs(channels, _TimeRange(0, 1_000_000_000), "epoch_seconds")

    assert len(jobs) == 1
    (slice_jobs,) = jobs.values()
    assert len(slice_jobs) == 1
    job = slice_jobs[0]
    assert sorted(job.channel_names) == ["a", "b"]
    assert job.datasource_rid == "ds1"
    assert job.timestamp_type == "epoch_seconds"
    assert job.compression is None  # propagated from the handler


def test_compute_export_jobs_subdivides_large_channels() -> None:
    """A channel whose rate exceeds the per-request budget is subdivided across the time slice."""
    handler = _handler(points_per_request=100, channels_per_request=10)
    channels = [_ch("big"), _ch("a")]

    with patch.object(handler, "_compute_channel_points_per_second", return_value={"big": 1000.0, "a": 1.0}):
        jobs = handler._compute_export_jobs(
            channels,
            _TimeRange(0, 1_000_000_000),  # 1s
            "epoch_seconds",
            batch_duration=datetime.timedelta(seconds=1),  # single 1s slice
        )

    (slice_jobs,) = jobs.values()
    big_jobs = [j for j in slice_jobs if j.channel_names == ["big"]]
    a_jobs = [j for j in slice_jobs if j.channel_names == ["a"]]
    # allowed_rate=100, big=1000 -> sub_offset = 100/1000 = 0.1s -> 10 sub-slices over a 1s slice
    assert len(big_jobs) == 10
    assert len(a_jobs) == 1


def test_compute_export_jobs_requires_batch_duration_without_numeric_channels() -> None:
    """Enum-only exports must be given an explicit batch_duration since rate cannot be estimated."""
    handler = _handler()
    channels = [_ch("e", data_type=ChannelDataType.STRING)]

    with pytest.raises(ValueError, match="a `batch_duration` must be provided"):
        handler._compute_export_jobs(channels, _TimeRange(0, 1_000_000_000), "epoch_seconds")


# ---- _ExportJob.export_request ----


@pytest.mark.parametrize(
    "compression",
    [None, scout_dataexport_api.CompressionFormat.GZIP],
)
def test_export_request_honors_compression(compression: scout_dataexport_api.CompressionFormat | None) -> None:
    """export_request threads the job's compression setting into the conjure request."""
    job = _ExportJob(
        datasource_rid="ds1",
        channel_names=["a"],
        channel_types={"a": ChannelDataType.DOUBLE},
        time_slice=_TimeRange(0, 1_000_000_000),
        tags={},
        compression=compression,
    )
    datasource = MagicMock()
    datasource.get_channels.return_value = [MagicMock()]

    request = job.export_request(cast(DataSource, datasource))

    assert request.compression is compression
    assert request.format.csv is not None
