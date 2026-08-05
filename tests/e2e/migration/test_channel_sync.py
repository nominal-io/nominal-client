r"""End-to-end tests for channel-data sync (``nominal.experimental.migration.channel_sync``).

These call the channel-sync entrypoints directly (``sync_missing_channel_data`` /
``sync_missing_channel_data_for_tag_filters``) rather than ``MigrationRunner`` — channel sync moves
*point data* a destination dataset is missing, a separate concern from MigrationRunner's
metadata/dataset-file copy. The export path they exercise is the presigned, parallel multipart
downloader, so this suite is also the end-to-end coverage for that utility.

The selection mechanism under test is the datascope **tag filter**: a tagged sync must copy exactly
the matching series and leave the others untouched. The two-tag source fixture
(``source_dataset_two_tags``) carries the same channels under ``asset_id=A`` and ``asset_id=B`` over
partially overlapping time ranges (see the layout in ``conftest.py``); the sync window spans both, so
the *tag filter* — not the time window — is the only thing that can exclude the other tag, and a leak
shows up as the wrong tag's distinct values landing in the destination.

Eventual consistency
--------------------
Streaming ingestion settles asynchronously. The sync's own settle/re-detect loop covers the sync
itself, but post-sync *verification* reads can briefly lag, so the assertion helpers below poll the
destination with a bounded retry (never a fixed sleep) before failing.

Run with:
    uv run pytest tests/e2e/migration/test_channel_sync.py \
        --source-profile=<prod> --dest-profile=<staging> -v
"""

from __future__ import annotations

import json
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
from conjure_python_client import ConjureHTTPError

from nominal.core import NominalClient
from nominal.core.channel import Channel
from nominal.core.dataset import Dataset
from nominal.experimental.migration.channel_sync import (
    ChannelSyncOptions,
    sync_missing_channel_data,
    sync_missing_channel_data_for_tag_filters,
)
from nominal.thirdparty.pandas import channel_to_series
from tests.e2e import POLL_INTERVAL
from tests.e2e.migration.conftest import (
    FILE_POINT_COUNT,
    HALF_POINT_COUNT,
    LOG_CHANNEL_NAME,
    LOG_NUMERIC_CHANNEL,
    LOG_WINDOW_END,
    LOG_WINDOW_START,
    STRESS_CHANNEL_TYPES,
    STRESS_ENUM_VALUES,
    STRESS_ROWS,
    STRESS_WINDOW_END,
    STRESS_WINDOW_START,
    SYNC_TAG_A,
    SYNC_TAG_B,
    SYNC_TAG_KEY,
    SYNC_WINDOW_END,
    SYNC_WINDOW_START,
)

# The channels carried by the source CSVs (the timestamp column is not a channel).
SYNC_CHANNELS = ("temperature", "humidity", "relative_minutes")
# Each tag carries FILE_POINT_COUNT points per channel (sync_csv_a under A, sync_csv_b under B).
A_POINT_COUNT = FILE_POINT_COUNT
B_POINT_COUNT = FILE_POINT_COUNT

# Sub-window used to verify the high-cardinality STRING. The stress data is one row per second, so a
# one-minute window is ~60 distinct labels -- far under the backend export limits, unlike a full-window
# read of all STRESS_ROWS labels (which hits Compute:TooManyCategories / ServerOverloaded).
_HI_CARD_PROBE_NS = 60 * 1_000_000_000

# Give asynchronous streaming ingestion time to settle inside the sync before it re-detects.
SETTLE_SECONDS = 20.0
# Bounded retry for post-sync verification reads (data can lag the sync's own settle briefly).
_READ_RETRY_ATTEMPTS = 30
_READ_RETRY_DELAY = 2.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**overrides: Any) -> ChannelSyncOptions:
    """ChannelSyncOptions with test defaults (no progress bar, modest settle) plus overrides."""
    base: dict[str, Any] = {"show_progress": False, "settle_seconds": SETTLE_SECONDS}
    base.update(overrides)
    return ChannelSyncOptions(**base)


def _dest_channel(dest_dataset: Dataset, name: str) -> Channel:
    """Fetch a destination channel, retrying while its metadata is still propagating after streaming."""
    last_exc: Exception | None = None
    for _ in range(_READ_RETRY_ATTEMPTS):
        try:
            return dest_dataset.get_channel(name)
        except ValueError as exc:  # get_channel raises ValueError when the channel is not yet visible
            last_exc = exc
            time.sleep(_READ_RETRY_DELAY)
    raise AssertionError(f"channel {name!r} never appeared on the destination dataset: {last_exc}")


def _read_all_series(channel: Channel, start: int, end: int, tags: Mapping[str, str] | None) -> "pd.Series[Any]":
    """Read a channel over ``[start, end)`` as an index-sorted Series, working around the export
    category cap.

    A single export of a high-cardinality STRING channel fails with ``Compute:TooManyCategories`` once
    a window holds more distinct labels than the backend's per-request cap (``maxCategories``). When
    that happens we recursively halve the window and concatenate — the same recursive-halving the sync
    itself uses — so the verification read stays correct regardless of the cap's value. Sub-window
    boundaries are de-duplicated by timestamp in case the export range is inclusive.
    """
    try:
        return channel_to_series(channel, start, end, tags=tags).sort_index()
    except ConjureHTTPError as exc:
        if "TooManyCategories" not in str(exc) or end - start <= 1:
            raise
        mid = start + (end - start) // 2
        combined = pd.concat([_read_all_series(channel, start, mid, tags), _read_all_series(channel, mid, end, tags)])
        return combined[~combined.index.duplicated(keep="first")].sort_index()


def _read_all_values(channel: Channel, start: int, end: int, tags: Mapping[str, str] | None = None) -> list[Any]:
    """Index-sorted values of a channel over ``[start, end)`` (cap-safe; see :func:`_read_all_series`)."""
    return _read_all_series(channel, start, end, tags).to_list()


def _read_until(
    channel: Channel,
    start: int,
    end: int,
    tags: Mapping[str, str] | None,
    min_count: int,
) -> list[Any]:
    """Read a channel's values for ``tags`` over the window, polling until at least ``min_count`` land.

    Returns the values (index-sorted) of the last read; the caller asserts the final count so a
    shortfall surfaces as a clear length mismatch rather than a timeout.
    """
    values: list[Any] = []
    for _ in range(_READ_RETRY_ATTEMPTS):
        values = _read_all_values(channel, start, end, tags)
        if len(values) >= min_count:
            return values
        time.sleep(_READ_RETRY_DELAY)
    return values


def _read_until_series(
    channel: Channel,
    start: int,
    end: int,
    tags: Mapping[str, str] | None,
    min_unique: int,
) -> "pd.Series[Any]":
    """Read a channel as a Series, polling until it holds at least ``min_unique`` distinct timestamps.

    Used where the destination may hold duplicate points (bucket-granular re-streaming appends
    duplicates for a partially-present bucket), so callers assert timestamp *coverage* rather than an
    exact count.
    """
    series = _read_all_series(channel, start, end, tags)
    for _ in range(_READ_RETRY_ATTEMPTS):
        if series.index.nunique() >= min_unique:
            return series
        time.sleep(_READ_RETRY_DELAY)
        series = _read_all_series(channel, start, end, tags)
    return series


def _source_values(source_dataset: Dataset, name: str, tags: Mapping[str, str]) -> list[Any]:
    """Index-sorted values of a source channel for ``tags`` over the full sync window."""
    return _read_all_values(source_dataset.get_channel(name), SYNC_WINDOW_START, SYNC_WINDOW_END, tags)


def _assert_round_trip(
    source_dataset: Dataset,
    dest_dataset: Dataset,
    name: str,
    tags: Mapping[str, str],
    expected_count: int,
) -> None:
    """Assert a destination channel holds exactly the source channel's values for ``tags``."""
    expected = _source_values(source_dataset, name, tags)
    assert len(expected) == expected_count, f"source {name} has {len(expected)} points, expected {expected_count}"
    dest_channel = _dest_channel(dest_dataset, name)
    actual = _read_until(dest_channel, SYNC_WINDOW_START, SYNC_WINDOW_END, tags, expected_count)
    assert len(actual) == expected_count, f"dest {name} has {len(actual)} points, expected {expected_count}"
    assert actual == expected, f"dest {name} values do not match source for tags={dict(tags)}"


def _stress_dest_values(dest_dataset: Dataset, name: str) -> list[Any]:
    """Read an untagged destination channel over the stress window, polling until all rows land."""
    return _read_until(_dest_channel(dest_dataset, name), STRESS_WINDOW_START, STRESS_WINDOW_END, None, STRESS_ROWS)


def _available_tag_values(dest_dataset: Dataset, name: str) -> set[str]:
    """The ``asset_id`` tag values present on a destination channel over the window."""
    channel = _dest_channel(dest_dataset, name)
    available = channel.get_available_tags(SYNC_WINDOW_START, SYNC_WINDOW_END)
    return set(available.get(SYNC_TAG_KEY, set()))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_tag_filter_copies_only_matching_series(
    source_dataset_two_tags: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
):
    """A tag-filtered sync copies exactly the matching tagged series and nothing else.

    Syncs ``asset_id=A`` over the window spanning both A's and B's (overlapping) ranges. Because the
    window covers B's data too, only the tag filter can exclude B — so finding solely A's values in the
    destination (and only ``asset_id=A`` on the destination channels) confirms the filter, not the time
    window, did the selecting.
    """
    report = sync_missing_channel_data(
        source_dataset_two_tags,
        source_client,
        dest_dataset,
        SYNC_WINDOW_START,
        SYNC_WINDOW_END,
        _options(tags={SYNC_TAG_KEY: SYNC_TAG_A}),
    )

    assert report.points_streamed > 0
    assert report.channels_synced > 0
    assert report.still_short == []

    for name in SYNC_CHANNELS:
        _assert_round_trip(source_dataset_two_tags, dest_dataset, name, {SYNC_TAG_KEY: SYNC_TAG_A}, A_POINT_COUNT)
        # Only tag A landed: B was never copied despite having data inside the window.
        assert _available_tag_values(dest_dataset, name) == {SYNC_TAG_A}, f"{name} leaked a non-A tag"


def test_phase_plan_reports_ranges_and_leaves_dest_empty(
    source_dataset_two_tags: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
):
    """``phase="plan"`` reports the ranges a full run would sync without touching the destination."""
    report = sync_missing_channel_data(
        source_dataset_two_tags,
        source_client,
        dest_dataset,
        SYNC_WINDOW_START,
        SYNC_WINDOW_END,
        _options(tags={SYNC_TAG_KEY: SYNC_TAG_A}, phase="plan"),
    )

    assert report.planned_ranges, "plan should report missing ranges against an empty destination"
    for name in SYNC_CHANNELS:
        assert name in report.planned_ranges
    assert report.points_streamed == 0
    # The destination was not written to: no channels (and therefore no series) exist on it.
    assert list(dest_dataset.search_channels()) == []


def test_download_then_stream_phases_round_trip(
    source_dataset_two_tags: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
    tmp_path: Path,
):
    """``phase="download"`` then ``phase="stream"`` round-trips through files on disk.

    Download writes the missing ranges (plus a ``sync_tags.json``) into a per-filter subdirectory and
    streams nothing; a later stream pass auto-discovers that subdirectory from ``sync_tags.json`` —
    needing no source client or re-specified tags — and ingests the files into the destination.
    """
    out_dir = tmp_path / "export"
    subdir = out_dir / f"{SYNC_TAG_KEY}_{SYNC_TAG_A}"

    # --- download: files land on disk, destination untouched ---
    sync_missing_channel_data_for_tag_filters(
        source_dataset_two_tags,
        source_client,
        dest_dataset,
        SYNC_WINDOW_START,
        SYNC_WINDOW_END,
        tag_filters=[{SYNC_TAG_KEY: SYNC_TAG_A}],
        base_options=_options(phase="download", output_dir=out_dir),
    )
    assert (subdir / "sync_tags.json").exists()
    assert json.loads((subdir / "sync_tags.json").read_text()) == {SYNC_TAG_KEY: SYNC_TAG_A}
    assert list(subdir.glob("*.csv.gz")), "download phase produced no exported CSVs"
    assert list(dest_dataset.search_channels()) == [], "download phase must not write to the destination"

    # --- stream: auto-discover the subdir from sync_tags.json, no source needed ---
    reports = sync_missing_channel_data_for_tag_filters(
        None,
        None,
        dest_dataset,
        SYNC_WINDOW_START,
        SYNC_WINDOW_END,
        tag_filters=None,  # auto-discovered from sync_tags.json under out_dir
        base_options=_options(phase="stream", output_dir=out_dir),
    )
    assert len(reports) == 1
    assert reports[0].points_streamed > 0

    for name in SYNC_CHANNELS:
        _assert_round_trip(source_dataset_two_tags, dest_dataset, name, {SYNC_TAG_KEY: SYNC_TAG_A}, A_POINT_COUNT)


def test_partial_shortfall_fills_only_missing_buckets(
    source_dataset_two_tags: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
    sync_csv_a: bytes,
):
    """A partially-present destination has only its missing buckets filled (resumability).

    Pre-ingests tag A's first three (of six) detection buckets into the destination, then syncs tag A.
    The sync must move fewer points than a full (empty-destination) sync would -- i.e. it reused the
    pre-loaded buckets rather than re-copying the whole window -- and the destination must end covering
    every source point.

    The exact streamed count is not asserted: ``points_streamed`` is cumulative across the
    settle/re-detect retries, and under eventual consistency the loop can re-stream a not-yet-visible
    bucket (append-only, so a re-streamed present bucket also leaves duplicate points). Correctness is
    therefore asserted as timestamp *coverage*, tolerant of those duplicates.
    """
    # Tag A's first three buckets: header + HALF_POINT_COUNT data rows (an exact bucket boundary).
    partial_csv = b"\n".join(sync_csv_a.split(b"\n")[: HALF_POINT_COUNT + 1]) + b"\n"
    dest_dataset.add_from_io(
        BytesIO(partial_csv), "timestamp", "iso_8601", tags={SYNC_TAG_KEY: SYNC_TAG_A}
    ).poll_until_ingestion_completed(interval=POLL_INTERVAL)

    report = sync_missing_channel_data(
        source_dataset_two_tags,
        source_client,
        dest_dataset,
        SYNC_WINDOW_START,
        SYNC_WINDOW_END,
        _options(tags={SYNC_TAG_KEY: SYNC_TAG_A}),
    )

    assert report.still_short == []
    # The pre-load was reused: fewer points streamed than a from-scratch sync of the full window.
    assert 0 < report.points_streamed < FILE_POINT_COUNT * len(SYNC_CHANNELS)

    # The destination covers every source point for tag A (dedup-tolerant: re-streaming may duplicate).
    tags_a = {SYNC_TAG_KEY: SYNC_TAG_A}
    for name in SYNC_CHANNELS:
        src = _read_all_series(source_dataset_two_tags.get_channel(name), SYNC_WINDOW_START, SYNC_WINDOW_END, tags_a)
        dest = _read_until_series(
            _dest_channel(dest_dataset, name), SYNC_WINDOW_START, SYNC_WINDOW_END, tags_a, A_POINT_COUNT
        )
        assert set(src.index).issubset(dest.index), f"{name}: destination is missing source timestamps"


def test_multi_tag_filters_copy_both_into_subdirs(
    source_dataset_two_tags: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
    tmp_path: Path,
):
    """``sync_missing_channel_data_for_tag_filters`` over two filters copies both tags' series.

    Each filter exports into its own ``output_dir/<tag_label>`` subdirectory and streams into the
    destination, so afterwards both ``asset_id`` values are present with their own distinct data.
    """
    out_dir = tmp_path / "export"

    reports = sync_missing_channel_data_for_tag_filters(
        source_dataset_two_tags,
        source_client,
        dest_dataset,
        SYNC_WINDOW_START,
        SYNC_WINDOW_END,
        tag_filters=[{SYNC_TAG_KEY: SYNC_TAG_A}, {SYNC_TAG_KEY: SYNC_TAG_B}],
        base_options=_options(output_dir=out_dir),
    )

    assert len(reports) == 2
    assert all(r.points_streamed > 0 for r in reports)
    # Each filter exported into its own subdirectory.
    assert list((out_dir / f"{SYNC_TAG_KEY}_{SYNC_TAG_A}").glob("*.csv.gz"))
    assert list((out_dir / f"{SYNC_TAG_KEY}_{SYNC_TAG_B}").glob("*.csv.gz"))

    for name in SYNC_CHANNELS:
        # Both tag values landed, each with its own source values.
        assert _available_tag_values(dest_dataset, name) == {SYNC_TAG_A, SYNC_TAG_B}
        _assert_round_trip(source_dataset_two_tags, dest_dataset, name, {SYNC_TAG_KEY: SYNC_TAG_A}, A_POINT_COUNT)
        _assert_round_trip(source_dataset_two_tags, dest_dataset, name, {SYNC_TAG_KEY: SYNC_TAG_B}, B_POINT_COUNT)


def test_adversarial_channel_types_round_trip(
    source_dataset_stress: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
):
    """The migration-hard channel types each round-trip with values and types preserved.

    Exercises the non-numeric / non-precise code paths end-to-end: a high-cardinality STRING channel
    overflows the enum-category limit and takes the recursive-halving export fallback, a low-card enum
    STRING re-reads as strings, and integer-formatted (``int_ch``) and decimal-formatted (``dbl_ch``)
    numeric columns both stay DOUBLE -- the Float64 guard must not re-infer the integer-formatted one
    as INT in the destination. (CSV ingest produces no genuine INT channel, so the INT->int recast is
    unit-tested rather than here.) ``ChannelSyncReport`` does not expose which channels took the
    non-precise fallback, so this asserts the *outcome* -- every value and the channel type survive --
    rather than the path taken.
    """
    report = sync_missing_channel_data(
        source_dataset_stress,
        source_client,
        dest_dataset,
        STRESS_WINDOW_START,
        STRESS_WINDOW_END,
        _options(),
    )

    assert report.points_streamed > 0
    assert report.still_short == []

    # Types are preserved for every channel.
    for name, expected_type in STRESS_CHANNEL_TYPES.items():
        dest_channel = _dest_channel(dest_dataset, name)
        assert dest_channel.data_type is not None, f"{name} has no destination data type"
        assert dest_channel.data_type.value == expected_type, (
            f"{name} landed as {dest_channel.data_type.value}, expected {expected_type}"
        )

    # Full-window value round-trip for the channels a full read can return: the numeric channels and
    # the low-cardinality enum. The high-cardinality STRING is verified over a small sub-window below --
    # a full-window read of all STRESS_ROWS distinct labels is expensive enough to hit the backend
    # export limits (Compute:TooManyCategories / ServerOverloaded), which is exactly why that channel
    # takes the non-precise path.
    for name in ("enum_str", "int_ch", "dbl_ch"):
        src_values = _read_all_values(source_dataset_stress.get_channel(name), STRESS_WINDOW_START, STRESS_WINDOW_END)
        dest_channel = _dest_channel(dest_dataset, name)
        dest_values = sorted(_read_until(dest_channel, STRESS_WINDOW_START, STRESS_WINDOW_END, None, STRESS_ROWS))
        assert len(dest_values) == STRESS_ROWS, f"{name} landed {len(dest_values)} points, expected {STRESS_ROWS}"
        assert dest_values == sorted(src_values), f"{name} values do not round-trip"

    # enum_str kept exactly its label set (values stay strings, not coerced to numbers).
    assert set(_stress_dest_values(dest_dataset, "enum_str")) == set(STRESS_ENUM_VALUES)

    # High-cardinality STRING: verify a small sub-window (far under the export limits) round-trips with
    # one unique value per row. Confirms the non-precise / recursive-halving export path moved the data;
    # the full window is not reliably readable via channel_to_series, and the sync's own success
    # (points_streamed > 0, still_short == []) already covers the full set landing.
    probe_end = STRESS_WINDOW_START + _HI_CARD_PROBE_NS
    src_hi_channel = source_dataset_stress.get_channel("hi_card_str")
    src_hi = _read_all_series(src_hi_channel, STRESS_WINDOW_START, probe_end, None).to_list()
    assert len(src_hi) > 0, "high-cardinality probe window read no source data"
    dest_hi = _read_until(_dest_channel(dest_dataset, "hi_card_str"), STRESS_WINDOW_START, probe_end, None, len(src_hi))
    assert sorted(dest_hi) == sorted(src_hi), "hi_card_str values do not round-trip over the probe window"
    assert len(set(dest_hi)) == len(src_hi), "hi_card_str should have one unique value per row"


def test_untagged_high_cardinality_channel_detected_present(
    source_dataset_stress: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
):
    """An untagged high-cardinality STRING channel is detected present via the series-count probe.

    ``hi_card_str`` overflows the enum-category limit during detection, so it cannot be counted
    precisely and falls to the whole-window presence probe. That probe is now tag-independent
    (``batchGetSeriesCount`` / ``series_count > 0``) rather than tag-derived, so it must flag the
    channel present and sync it even though nothing constrains it by tags. This locks in that the
    series-count probe detects untagged data (``tags=None``); the genuinely tag-free correctness --
    which e2e cannot construct, since exportable data always carries internal ``_nominal_*`` tags --
    is proven by the ``_channels_with_data`` unit tests.
    """
    report = sync_missing_channel_data(
        source_dataset_stress,
        source_client,
        dest_dataset,
        STRESS_WINDOW_START,
        STRESS_WINDOW_END,
        _options(),  # tags=None
    )

    # The presence probe found data for the non-precise channel: it synced and nothing is still short.
    assert report.channels_synced > 0
    assert report.still_short == []

    # The untagged high-cardinality channel actually landed (verified over a cap-safe sub-window).
    probe_end = STRESS_WINDOW_START + _HI_CARD_PROBE_NS
    src_hi = _read_all_series(source_dataset_stress.get_channel("hi_card_str"), STRESS_WINDOW_START, probe_end, None)
    assert src_hi.index.nunique() > 0, "high-cardinality probe window read no source data"
    dest_hi = _read_until(
        _dest_channel(dest_dataset, "hi_card_str"), STRESS_WINDOW_START, probe_end, None, src_hi.index.nunique()
    )
    assert sorted(dest_hi) == sorted(src_hi.to_list()), "untagged hi_card_str did not round-trip"


def test_log_channel_skipped_and_absent_on_dest(
    source_dataset_with_log: Dataset,
    source_client: NominalClient,
    dest_dataset: Dataset,
):
    """A LOG channel is skipped as unsupported: counted in the report, never written to the destination.

    LOG is not an exportable channel type, so sync drops it *before* detection
    (``report.channels_skipped_unsupported``) while still moving the supported numeric channels. The
    LOG channel must never become visible on the destination dataset.
    """
    report = sync_missing_channel_data(
        source_dataset_with_log,
        source_client,
        dest_dataset,
        LOG_WINDOW_START,
        LOG_WINDOW_END,
        _options(),
    )

    # The LOG channel was skipped as unsupported, but supported channels still synced.
    assert report.channels_skipped_unsupported >= 1
    assert report.channels_synced > 0
    assert report.still_short == []

    dest_channel_names = {channel.name for channel in dest_dataset.search_channels()}
    # A supported numeric channel round-tripped; the LOG channel never did.
    assert LOG_NUMERIC_CHANNEL in dest_channel_names, "the supported numeric channel should have synced"
    assert LOG_CHANNEL_NAME not in dest_channel_names, "the LOG channel must not appear on the destination"
