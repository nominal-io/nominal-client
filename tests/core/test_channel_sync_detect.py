from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.core.channel import ChannelDataType
from nominal.experimental.migration.channel_sync import detect as detect_mod
from nominal.experimental.migration.channel_sync.detect import (
    ChannelBucketCounts,
    _iter_bucket_starts,
    count_channels,
    merge_bucket_ranges,
    shortfall_buckets,
)

SEC = 1_000_000_000  # one second in nanoseconds


def _ts(nanos_total: int) -> SimpleNamespace:
    seconds, nanos = divmod(nanos_total, SEC)
    return SimpleNamespace(seconds=seconds, nanos=nanos)


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

    def fake_channels_with_data(chs: Any, start: int, end: int, tags: Any, on_advance: Any) -> set[str]:
        # Mirror the real probe's on_advance contract (advanced by batch length) and report none
        # present, so every presence channel bins to a uniform 0 (precise=False) count.
        for ch in chs:
            presence_seen.append(ch.name)
        if on_advance is not None:
            on_advance(len(list(chs)))
        return set()

    monkeypatch.setattr(detect_mod, "_count_chunk", fake_count_chunk)
    monkeypatch.setattr(detect_mod, "_channels_with_data", fake_channels_with_data)

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


# --- count_channels: long-window segmentation (backend 10k-bucket cap) --------------------


def test_count_channels_segments_long_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    """A window over MAX_BUCKETS_PER_REQUEST buckets splits into per-segment requests whose counts
    merge into one complete precise result (the backend 400s any request beyond the cap).
    """
    monkeypatch.setattr(detect_mod, "MAX_BUCKETS_PER_REQUEST", 4)
    ch = SimpleNamespace(name="c", data_type=ChannelDataType.DOUBLE)

    seen: list[tuple[int, int, int]] = []

    def fake_count_chunk(
        chunk: Any, start: int, end: int, bucket: int, starts: Any, tags: Any
    ) -> tuple[dict[str, ChannelBucketCounts], list[Any]]:
        seen.append((start, end, len(starts)))
        return {"c": ChannelBucketCounts("c", dict.fromkeys(starts, 2), precise=True)}, []

    monkeypatch.setattr(detect_mod, "_count_chunk", fake_count_chunk)

    advanced = 0

    def on_advance(n: int) -> None:
        nonlocal advanced
        advanced += n

    # 10 one-second buckets with a 4-bucket cap -> segments of 4, 4, and 2 buckets.
    result = count_channels([ch], 0, 10 * SEC, SEC, on_advance=on_advance)

    assert sorted(seen) == [(0, 4 * SEC, 4), (4 * SEC, 8 * SEC, 4), (8 * SEC, 10 * SEC, 2)]
    assert result["c"].precise is True
    # Merged counts cover every bucket of the full window exactly once.
    assert result["c"].counts == dict.fromkeys(range(0, 10 * SEC, SEC), 2)
    # The channel advances exactly once over the whole call (first segment only).
    assert advanced == 1


def test_count_channels_errored_in_any_segment_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """A channel whose batch errored in one segment must not surface a partial precise result --
    it routes to the fallback for the whole window.
    """
    monkeypatch.setattr(detect_mod, "MAX_BUCKETS_PER_REQUEST", 4)
    ch = SimpleNamespace(name="c", data_type=ChannelDataType.DOUBLE)

    def fake_count_chunk(
        chunk: Any, start: int, end: int, bucket: int, starts: Any, tags: Any
    ) -> tuple[dict[str, ChannelBucketCounts], list[Any]]:
        if start == 4 * SEC:  # second segment errors
            return {}, list(chunk)
        return {"c": ChannelBucketCounts("c", dict.fromkeys(starts, 2), precise=True)}, []

    monkeypatch.setattr(detect_mod, "_count_chunk", fake_count_chunk)
    monkeypatch.setattr(detect_mod, "_channels_with_data", lambda *a, **k: {"c"})

    result = count_channels([ch], 0, 10 * SEC, SEC)

    assert result["c"].precise is False
    # Whole-window presence fill, not the partial per-segment counts.
    assert result["c"].counts == dict.fromkeys(range(0, 10 * SEC, SEC), 1)


# --- count_channels: presence_fallback="empty" (destination side) --------------------------


def test_count_channels_presence_fallback_empty_skips_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """With presence_fallback="empty", imprecisely-countable channels read as all-zero (fully
    missing) and no presence probe is issued -- a probe finding any data would mask real gaps.
    """
    err = SimpleNamespace(name="err", data_type=ChannelDataType.DOUBLE)
    log = SimpleNamespace(name="log0", data_type=ChannelDataType.LOG)

    def fake_count_chunk(
        chunk: Any, start: int, end: int, bucket: int, starts: Any, tags: Any
    ) -> tuple[dict[str, ChannelBucketCounts], list[Any]]:
        return {}, list(chunk)

    monkeypatch.setattr(detect_mod, "_count_chunk", fake_count_chunk)
    monkeypatch.setattr(
        detect_mod,
        "_channels_with_data",
        lambda *a, **k: pytest.fail("presence probe must not run in 'empty' mode"),
    )

    advanced = 0

    def on_advance(n: int) -> None:
        nonlocal advanced
        advanced += n

    result = count_channels([err, log], 0, 2 * SEC, SEC, on_advance=on_advance, presence_fallback="empty")

    assert result["err"].precise is False
    assert result["err"].counts == {0: 0, SEC: 0}
    assert result["log0"].precise is False
    assert result["log0"].counts == {0: 0, SEC: 0}
    # Both channels advance exactly once over the call (err in the chunk pass, log0 in fallback).
    assert advanced == 2


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


# --- _channels_with_data: tag-independent presence via batchGetSeriesCount ------------------
# The presence probe keys off series_count, not tags -- these mock channels carry no tags at all,
# so they prove the probe detects genuinely tag-free data (which e2e can't create).


def test_channels_with_data_presence_by_series_count(
    make_channel: Any, mock_clients: Any, make_series_count_response: Any
) -> None:
    # count>0 -> present; count 0 -> absent; None (external datasource, series_count absent) -> absent.
    channels = [make_channel("c_pos"), make_channel("c_zero"), make_channel("c_ext"), make_channel("c_pos2")]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([5, 0, None, 1])

    advanced = 0

    def on_advance(n: int) -> None:
        nonlocal advanced
        advanced += n

    result = detect_mod._channels_with_data(channels, 0, SEC, None, on_advance)

    assert result == {"c_pos", "c_pos2"}
    assert advanced == 4


def test_channels_with_data_batch_failure_is_conservatively_present(make_channel: Any, mock_clients: Any) -> None:
    # A probe failure must never drop real data: treat the whole batch as present so it is (re)synced.
    channels = [make_channel("a"), make_channel("b")]
    mock_clients.datasource.batch_get_series_count.side_effect = RuntimeError("probe boom")

    assert detect_mod._channels_with_data(channels, 0, SEC, None, None) == {"a", "b"}


def test_channels_with_data_empty_returns_empty_set() -> None:
    assert detect_mod._channels_with_data([], 0, SEC, None, None) == set()


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


# --- count_channels: high-cardinality STRING channels counted by halving --------------------

_MAX_CATEGORIES = 4  # stands in for the backend's maxCategories


def _string_compute(points: list[int], error_type: str = "Compute:TooManyCategories") -> SimpleNamespace:
    """A fake compute service over one STRING channel holding a distinct value at each of ``points``.

    A request window holding more than ``_MAX_CATEGORIES`` points fails with ``error_type``. Otherwise
    it succeeds with one enum bucket per point. Each request's ``(start, end)`` is recorded.
    """
    windows: list[tuple[int, int]] = []

    def batch_compute_with_units(auth: Any, request: Any) -> SimpleNamespace:
        results = []
        for req in request.requests:
            windows.append((req.start, req.end))
            inside = [p for p in points if req.start <= p < req.end]
            if len(inside) > _MAX_CATEGORIES:
                error = SimpleNamespace(code=400, error_type=error_type)
                results.append(SimpleNamespace(compute_result=SimpleNamespace(success=None, error=error)))
            else:
                success = SimpleNamespace(type="bucketedEnum", points=inside)
                results.append(SimpleNamespace(compute_result=SimpleNamespace(success=success, error=None)))
        return SimpleNamespace(results=results)

    return SimpleNamespace(
        auth_header="auth", compute=SimpleNamespace(batch_compute_with_units=batch_compute_with_units), windows=windows
    )


@pytest.fixture
def string_channel_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Route request building and enum parsing through plain windows and point lists."""
    monkeypatch.setattr(
        detect_mod,
        "_bucket_request",
        lambda channel, tags, start, end, buckets: SimpleNamespace(start=start, end=end),
    )
    monkeypatch.setattr(
        detect_mod,
        "_enum_buckets_from_compute_response",
        lambda response: [
            SimpleNamespace(frequencies={f"v{p}": 1}, first_point=SimpleNamespace(timestamp=p)) for p in response.points
        ],
    )


@pytest.mark.usefixtures("string_channel_boundary")
def test_count_channels_counts_high_cardinality_string_by_halving() -> None:
    """A STRING channel with too many distinct values gets exact per-bucket counts from halved windows."""
    compute = _string_compute(list(range(0, 20 * SEC, SEC)))
    channel = SimpleNamespace(name="s", data_type=ChannelDataType.STRING, _clients=compute)

    result = count_channels([channel], 0, 20 * SEC, 5 * SEC, presence_fallback="empty")

    assert result["s"].counts == {0: 5, 5 * SEC: 5, 10 * SEC: 5, 15 * SEC: 5}
    assert result["s"].precise is True
    assert result["s"].high_cardinality is True
    # Windows wider than one bucket split on bucket edges.
    assert all(lo % (5 * SEC) == 0 for lo, hi in compute.windows if hi - lo > 5 * SEC)


@pytest.mark.usefixtures("string_channel_boundary")
def test_count_channels_string_with_other_error_takes_fallback() -> None:
    """A STRING channel that errors for any reason other than too many categories is not halved."""
    compute = _string_compute(list(range(0, 20 * SEC, SEC)), error_type="Compute:SomethingElse")
    channel = SimpleNamespace(name="s", data_type=ChannelDataType.STRING, _clients=compute)

    result = count_channels([channel], 0, 20 * SEC, 5 * SEC, presence_fallback="empty")

    assert result["s"].precise is False
    assert result["s"].counts == dict.fromkeys(range(0, 20 * SEC, 5 * SEC), 0)
    # One batched request, then one single-channel retry that also fails: no halving.
    assert len(compute.windows) == 2


@pytest.mark.usefixtures("string_channel_boundary")
def test_count_channels_string_halving_stops_at_request_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Halving gives up and takes the fallback once a channel reaches the per-channel request cap."""
    monkeypatch.setattr(detect_mod, "MAX_HALVING_REQUESTS_PER_CHANNEL", 3)
    compute = _string_compute(list(range(0, 20 * SEC, SEC)))
    channel = SimpleNamespace(name="s", data_type=ChannelDataType.STRING, _clients=compute)

    result = count_channels([channel], 0, 20 * SEC, 5 * SEC, presence_fallback="empty")

    assert result["s"].precise is False
    assert len(compute.windows) == 1 + 3  # the batched request plus the capped halving requests


@pytest.mark.parametrize(
    ("lo", "hi", "grid", "expected"),
    [
        (0, 20 * SEC, 0, 10 * SEC),  # several buckets: the bucket edge at the middle
        (0, 15 * SEC, 0, 10 * SEC),  # middle falls mid-bucket: the nearest bucket edge
        (2 * SEC, 17 * SEC, 2 * SEC, 12 * SEC),  # edges follow the window's grid, not the epoch
        (5 * SEC, 10 * SEC, 0, 7_500_000_000),  # one bucket: the exact middle
    ],
)
def test_halving_midpoint(lo: int, hi: int, grid: int, expected: int) -> None:
    """Halving splits on the bucket edge nearest the middle, and inside a lone bucket at its middle."""
    assert detect_mod._halving_midpoint(lo, hi, grid, 5 * SEC) == expected
