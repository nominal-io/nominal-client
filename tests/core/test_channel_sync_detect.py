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
    _count_per_bucket,
    _iter_bucket_starts,
    count_channels,
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


# --- _iter_bucket_starts -------------------------------------------------------------------


def test_iter_bucket_starts_even_division() -> None:
    assert _iter_bucket_starts(0, 3 * SEC, SEC) == [0, SEC, 2 * SEC]


def test_iter_bucket_starts_partial_final_bucket_included() -> None:
    assert _iter_bucket_starts(0, 5 * SEC // 2, SEC) == [0, SEC, 2 * SEC]


@pytest.mark.parametrize(
    ("start", "end", "bucket"),
    [(0, 10, 0), (0, 10, -1), (10, 10, 1), (10, 5, 1)],
)
def test_iter_bucket_starts_rejects_bad_ranges(start: int, end: int, bucket: int) -> None:
    with pytest.raises(ValueError):
        _iter_bucket_starts(start, end, bucket)


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


# --- _count_per_bucket ---------------------------------------------------------------------


def _numeric_bucket(first_point_ns: int, count: int) -> SimpleNamespace:
    """A decimated NumericBucket stand-in carrying a first_point at the given ns and a count."""
    return SimpleNamespace(count=count, first_point=SimpleNamespace(timestamp=_ts(first_point_ns)))


def test_count_per_bucket_numeric_bins_buckets_by_first_point() -> None:
    # Each decimated bucket is binned by its first_point (a real sample inside the bucket), not the
    # bucket's right-edge timestamp. Empty buckets are not emitted, so SEC stays 0.
    response = SimpleNamespace(
        bucketed_numeric=SimpleNamespace(buckets=[_numeric_bucket(0, 10), _numeric_bucket(2 * SEC, 3)]),
        numeric=None,
    )
    result = _count_per_bucket(_numeric_channel(response), 0, 3 * SEC, SEC, tags={"s": "daq"})
    assert result.precise is True
    assert result.counts == {0: 10, SEC: 0, 2 * SEC: 3}


def test_count_per_bucket_numeric_last_bucket_binned_by_first_point() -> None:
    # Data only in the last bucket: its first_point at 2SEC bins the count to the 2SEC bucket (not
    # dropped). Guards against the old right-edge shift that pushed a single bucket out of range.
    response = SimpleNamespace(
        bucketed_numeric=SimpleNamespace(buckets=[_numeric_bucket(2 * SEC, 42)]),
        numeric=None,
    )
    result = _count_per_bucket(_numeric_channel(response), 0, 3 * SEC, SEC, tags=None)
    assert result.counts == {0: 0, SEC: 0, 2 * SEC: 42}


def test_count_per_bucket_numeric_raw_fallback_bins_points() -> None:
    response = SimpleNamespace(
        bucketed_numeric=None,
        numeric=SimpleNamespace(timestamps=[_ts(0), _ts(SEC // 4), _ts(2 * SEC + 1)]),
    )
    result = _count_per_bucket(_numeric_channel(response), 0, 3 * SEC, SEC, tags=None)
    assert result.counts == {0: 2, SEC: 0, 2 * SEC: 1}


def test_count_per_bucket_string_present_maps_to_one_per_bucket() -> None:
    channel = MagicMock()
    channel.name = "state"
    channel.data_type = ChannelDataType.STRING
    channel.get_available_tags.return_value = {"source": {"daq"}}
    result = _count_per_bucket(channel, 0, 3 * SEC, SEC, tags={"source": "daq"})
    assert result.precise is False
    assert result.counts == {0: 1, SEC: 1, 2 * SEC: 1}
    channel.get_available_tags.assert_called_once_with(0, 3 * SEC, initial_tags={"source": "daq"})


def test_count_per_bucket_string_absent_maps_to_zero() -> None:
    channel = MagicMock()
    channel.name = "state"
    channel.data_type = ChannelDataType.STRING
    channel.get_available_tags.return_value = {}
    result = _count_per_bucket(channel, 0, 2 * SEC, SEC, tags={"source": "daq"})
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

    advanced = 0

    def on_advance(n: int) -> None:
        nonlocal advanced
        advanced += n

    result = count_channels(channels, 0, SEC, SEC, channels_per_request=100, workers=4, on_advance=on_advance)

    # Every input channel is represented.
    assert len(result) == 152
    assert result["c0"].precise is True
    # The errored batch channel and the non-batchable LOG channel both fall back to presence.
    assert result["err"].precise is False
    assert result["log0"].precise is False
    assert sorted(presence_seen) == ["err", "log0"]
    # 151 batchable channels -> chunks of 100 and 51.
    assert sorted(chunk_sizes) == [51, 100]
    # on_advance sums to every input channel exactly once (errored counted only in the presence pass).
    assert advanced == 152


# --- _count_chunk: result/request length mismatch -----------------------------------------


def test_count_chunk_raises_when_results_shorter_than_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    # The API contract is one result per requested channel. A short result list must fail loud rather
    # than letting zip silently drop the trailing channels (excluding them from the sync with no signal).
    clients = SimpleNamespace(
        auth_header="auth",
        compute=SimpleNamespace(
            # One well-formed (errored) result for two requested channels. Without the length check,
            # zip(strict=False) would quietly drop the second channel and return normally; the check
            # makes the mismatch raise instead.
            batch_compute_with_units=lambda auth, request: SimpleNamespace(
                results=[SimpleNamespace(compute_result=None)]
            ),
        ),
    )
    ch1 = SimpleNamespace(name="c1", _clients=clients)
    ch2 = SimpleNamespace(name="c2", _clients=clients)
    # Bypass real conjure request construction; only the result-count check is under test.
    monkeypatch.setattr(detect_mod, "_bucket_request", lambda *a, **k: object())

    with pytest.raises(RuntimeError, match="cannot map results to channels"):
        detect_mod._count_chunk([ch1, ch2], 0, SEC, SEC, [0], tags=None)


# --- _counts_from_response: match on the (camelCase) union discriminator -------------------
# These pin the response.type labels the match relies on: a mislabeled case would route to the
# `case _` -> None fallback (channel silently drops to a presence probe), failing these asserts.


def _numeric_response(type_label: str, first_points: list[tuple[SimpleNamespace, int]], monkeypatch: Any) -> Any:
    """A fake response whose numeric buckets carry a first_point at the given ts with the given count.

    The yielded (right-edge) bucket timestamp is deliberately bogus (_ts(0)) so the tests prove binning
    uses first_point, not the bucket's own timestamp.
    """
    monkeypatch.setattr(
        detect_mod,
        "_numeric_buckets_from_compute_response",
        lambda response: [
            (_ts(0), SimpleNamespace(count=count, first_point=SimpleNamespace(timestamp=fp_ts)))
            for fp_ts, count in first_points
        ],
    )
    return SimpleNamespace(type=type_label)


def _enum_response(type_label: str, buckets: list[SimpleNamespace], monkeypatch: Any) -> Any:
    """A fake response of the given discriminator whose enum buckets are returned as-is."""
    monkeypatch.setattr(detect_mod, "_enum_buckets_from_compute_response", lambda response: buckets)
    return SimpleNamespace(type=type_label)


@pytest.mark.parametrize("type_label", ["bucketedNumeric", "numeric", "numericPoint"])
def test_counts_from_response_numeric_bins_by_first_point(type_label: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # All numeric variants bin by the bucket's first_point (here SEC), ignoring the bucket's own
    # (right-edge) timestamp. count=7 lands in the SEC bucket regardless of variant.
    response = _numeric_response(type_label, [(_ts(SEC), 7)], monkeypatch)
    assert detect_mod._counts_from_response(response, [0, SEC, 2 * SEC], SEC, 0) == {0: 0, SEC: 7, 2 * SEC: 0}


@pytest.mark.parametrize("type_label", ["bucketedEnum", "enum"])
def test_counts_from_response_enum_bins_by_first_point(type_label: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Enum variants bin by first_point.timestamp (already nanoseconds); the count is the frequency sum.
    bucket = SimpleNamespace(frequencies={"a": 3, "b": 2}, first_point=SimpleNamespace(timestamp=SEC))
    response = _enum_response(type_label, [bucket], monkeypatch)
    assert detect_mod._counts_from_response(response, [0, SEC, 2 * SEC], SEC, 0) == {0: 0, SEC: 5, 2 * SEC: 0}


def test_counts_from_response_unknown_type_returns_none() -> None:
    # An untyped-for-our-purposes variant -> None, so the channel falls back to a presence probe.
    assert detect_mod._counts_from_response(SimpleNamespace(type="log"), [0, SEC], SEC, 0) is None


def test_counts_from_response_single_bucket_binned_by_first_point_not_right_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a single decimated bucket must be binned by its first_point, not (right_edge - bucket).

    Reproduces the prod->staging "nothing to sync" failure. With DecimateWithBuckets and buckets=1 the
    backend uses resolution = window+1ns grid-aligned to the epoch, so the single bucket's exclusive
    right-edge timestamp lands ~at start (a few ns/us after it), NOT at start+bucket. The real data
    (its first_point) is at start. Binning by ``right_edge - bucket`` underflows below start and drops
    the whole count; binning by first_point keeps it.
    """
    right_edge = _ts(1000)  # ~1us after start -- the backend's near-start right edge for buckets=1
    first_point = SimpleNamespace(timestamp=_ts(0))  # the real first sample, at start
    numeric_bucket = SimpleNamespace(count=10, first_point=first_point)
    monkeypatch.setattr(
        detect_mod, "_numeric_buckets_from_compute_response", lambda response: [(right_edge, numeric_bucket)]
    )
    response = SimpleNamespace(type="bucketedNumeric")

    # The 10 points must land in bucket 0 (where the data is), not be dropped to {0: 0}.
    assert detect_mod._counts_from_response(response, [0, SEC, 2 * SEC], SEC, 0) == {0: 10, SEC: 0, 2 * SEC: 0}
