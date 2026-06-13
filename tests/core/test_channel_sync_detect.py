from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.core.channel import ChannelDataType
from nominal.experimental.migration.channel_sync import detect as detect_mod
from nominal.experimental.migration.channel_sync.detect import (
    ChannelBucketCounts,
    count_channels,
    count_per_bucket,
    iter_bucket_starts,
    merge_bucket_ranges,
    shortfall_buckets,
)

SEC = 1_000_000_000  # one second in nanoseconds


def _ts(nanos_total: int) -> SimpleNamespace:
    seconds, nanos = divmod(nanos_total, SEC)
    return SimpleNamespace(seconds=seconds, nanos=nanos)


def _numeric_channel(response: SimpleNamespace) -> MagicMock:
    channel = MagicMock()
    channel.name = "rpm"
    channel.data_type = ChannelDataType.DOUBLE
    channel._decimate_request.return_value = response
    return channel


# --- iter_bucket_starts -------------------------------------------------------------------


def test_iter_bucket_starts_even_division() -> None:
    assert iter_bucket_starts(0, 3 * SEC, SEC) == [0, SEC, 2 * SEC]


def test_iter_bucket_starts_partial_final_bucket_included() -> None:
    assert iter_bucket_starts(0, 5 * SEC // 2, SEC) == [0, SEC, 2 * SEC]


@pytest.mark.parametrize(
    ("start", "end", "bucket"),
    [(0, 10, 0), (0, 10, -1), (10, 10, 1), (10, 5, 1)],
)
def test_iter_bucket_starts_rejects_bad_ranges(start: int, end: int, bucket: int) -> None:
    with pytest.raises(ValueError):
        iter_bucket_starts(start, end, bucket)


# --- merge_bucket_ranges ------------------------------------------------------------------


def test_merge_bucket_ranges_coalesces_adjacent() -> None:
    assert merge_bucket_ranges([0, SEC, 2 * SEC], SEC) == [(0, 3 * SEC)]


def test_merge_bucket_ranges_splits_gaps() -> None:
    # Gap at SEC -> two ranges.
    assert merge_bucket_ranges([0, 2 * SEC], SEC) == [(0, SEC), (2 * SEC, 3 * SEC)]


def test_merge_bucket_ranges_dedups_and_sorts() -> None:
    assert merge_bucket_ranges([2 * SEC, 0, 0, SEC], SEC) == [(0, 3 * SEC)]


def test_merge_bucket_ranges_empty() -> None:
    assert merge_bucket_ranges([], SEC) == []


# --- shortfall_buckets --------------------------------------------------------------------


def test_shortfall_buckets_flags_any_shortfall() -> None:
    src = ChannelBucketCounts("c", {0: 10, SEC: 5, 2 * SEC: 0}, precise=True)
    dest = ChannelBucketCounts("c", {0: 10, SEC: 4, 2 * SEC: 0}, precise=True)
    assert shortfall_buckets(src, dest) == [SEC]


def test_shortfall_buckets_treats_missing_dest_bucket_as_zero() -> None:
    src = ChannelBucketCounts("c", {0: 1, SEC: 1}, precise=True)
    dest = ChannelBucketCounts("c", {}, precise=True)
    assert shortfall_buckets(src, dest) == [0, SEC]


def test_shortfall_buckets_no_shortfall_when_dest_exceeds_src() -> None:
    src = ChannelBucketCounts("c", {0: 1}, precise=True)
    dest = ChannelBucketCounts("c", {0: 5}, precise=True)
    assert shortfall_buckets(src, dest) == []


# --- count_per_bucket ---------------------------------------------------------------------


def test_count_per_bucket_numeric_uses_bucketed_counts() -> None:
    response = SimpleNamespace(
        bucketed_numeric=SimpleNamespace(
            timestamps=[_ts(SEC // 2), _ts(SEC + SEC // 2), _ts(2 * SEC + SEC // 2)],
            buckets=[SimpleNamespace(count=10), SimpleNamespace(count=0), SimpleNamespace(count=3)],
        ),
        numeric=None,
    )
    result = count_per_bucket(_numeric_channel(response), 0, 3 * SEC, SEC, tags={"s": "daq"})
    assert result.precise is True
    assert result.counts == {0: 10, SEC: 0, 2 * SEC: 3}


def test_count_per_bucket_numeric_raw_fallback_bins_points() -> None:
    response = SimpleNamespace(
        bucketed_numeric=None,
        numeric=SimpleNamespace(timestamps=[_ts(0), _ts(SEC // 4), _ts(2 * SEC + 1)]),
    )
    result = count_per_bucket(_numeric_channel(response), 0, 3 * SEC, SEC, tags=None)
    assert result.counts == {0: 2, SEC: 0, 2 * SEC: 1}


def test_count_per_bucket_string_present_maps_to_one_per_bucket() -> None:
    channel = MagicMock()
    channel.name = "state"
    channel.data_type = ChannelDataType.STRING
    channel.get_available_tags.return_value = {"source": {"daq"}}
    result = count_per_bucket(channel, 0, 3 * SEC, SEC, tags={"source": "daq"})
    assert result.precise is False
    assert result.counts == {0: 1, SEC: 1, 2 * SEC: 1}
    channel.get_available_tags.assert_called_once_with(0, 3 * SEC, initial_tags={"source": "daq"})


def test_count_per_bucket_string_absent_maps_to_zero() -> None:
    channel = MagicMock()
    channel.name = "state"
    channel.data_type = ChannelDataType.STRING
    channel.get_available_tags.return_value = {}
    result = count_per_bucket(channel, 0, 2 * SEC, SEC, tags={"source": "daq"})
    assert result.counts == {0: 0, SEC: 0}


# --- count_channels: batching, fallback routing, threading --------------------------------


def test_count_channels_batches_and_routes_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    doubles = [SimpleNamespace(name=f"c{i}", data_type=ChannelDataType.DOUBLE) for i in range(150)]
    errored_channel = SimpleNamespace(name="err", data_type=ChannelDataType.DOUBLE)
    log_channel = SimpleNamespace(name="log0", data_type=ChannelDataType.LOG)  # non-batchable
    channels = [*doubles, errored_channel, log_channel]

    chunk_sizes: list[int] = []

    def fake_count_chunk(
        chunk: Any, start: int, end: int, bucket: int, starts: Any, tags: Any
    ) -> tuple[dict[str, ChannelBucketCounts], list[Any]]:
        chunk_sizes.append(len(chunk))
        counts: dict[str, ChannelBucketCounts] = {}
        errored: list[Any] = []
        for ch in chunk:
            if ch.name == "err":
                errored.append(ch)  # simulate a batch result that errored -> presence fallback
            else:
                counts[ch.name] = ChannelBucketCounts(ch.name, {0: 1}, precise=True)
        return counts, errored

    presence_seen: list[str] = []

    def fake_presence(ch: Any, start: int, end: int, starts: Any, tags: Any) -> dict[int, int]:
        presence_seen.append(ch.name)
        return {0: 0}

    monkeypatch.setattr(detect_mod, "_count_chunk", fake_count_chunk)
    monkeypatch.setattr(detect_mod, "_presence_counts", fake_presence)

    result = count_channels(channels, 0, SEC, SEC, channels_per_request=100, workers=4)

    # Every input channel is represented.
    assert len(result) == 152
    assert result["c0"].precise is True
    # The errored batch channel and the non-batchable LOG channel both fall back to presence.
    assert result["err"].precise is False
    assert result["log0"].precise is False
    assert sorted(presence_seen) == ["err", "log0"]
    # 151 batchable channels -> chunks of 100 and 51.
    assert sorted(chunk_sizes) == [51, 100]
