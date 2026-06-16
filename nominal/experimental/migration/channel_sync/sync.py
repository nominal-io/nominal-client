"""Sync the channel data a destination dataset is missing, from a source dataset.

For a single ``(source_dataset, destination_dataset)`` pair over a window, this:

1. Lists the source channels (:meth:`Dataset.search_channels`) and detects, per channel and per
   time bucket, which buckets the destination is short on (see :mod:`.detect`). A channel that does
   not exist in the destination, or exists but is empty over the window, both read as "missing".
2. Exports only the missing time-ranges from the source via the presigned, parallel-download path
   (:meth:`PolarsExportHandler.export_to_files`) to gzipped CSVs on disk.
3. Re-reads those CSVs and streams the points into the destination via
   :meth:`Dataset.get_write_stream` (which auto-creates the series if absent).
4. Waits for the asynchronous ingestion to settle, re-detects, and re-streams anything still short
   up to ``max_retries`` times. Whatever remains short is logged (channel + tags + time-slice) and
   recorded in the returned :class:`ChannelSyncReport`; it is non-fatal.

Resumability is implicit: detection is idempotent, so re-running simply re-syncs whatever is still
short. There is no state file.

Caveats:
* **Single tag-filter.** Each channel is treated as one series under the optional ``tags`` filter;
  channels carrying extra tag dimensions are not enumerated per-combination.
* **Bucket-granularity append.** A bucket that is *partially* present in the destination is
  re-streamed in full, which appends duplicate points for the portion already present. The common
  case -- an empty bucket (``dest_count == 0``) -- never duplicates. Streaming is append-only.
"""

from __future__ import annotations

import contextlib
import gzip
import logging
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Literal

import polars as pl

from nominal.core._stream.write_stream import DataStream
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset
from nominal.experimental.migration.channel_sync.detect import (
    DEFAULT_DETECT_CHANNELS_PER_REQUEST,
    DEFAULT_DETECT_WORKERS,
    ChannelBucketCounts,
    count_channels,
    merge_bucket_ranges,
    shortfall_buckets,
)
from nominal.thirdparty.polars.polars_export_handler import (
    _EXPORTABLE_DATA_TYPES,
    DEFAULT_CHANNELS_PER_REQUEST,
    DEFAULT_MAX_CONCURRENT_LINKS,
    DEFAULT_NUM_WORKERS,
    DEFAULT_POINTS_PER_DATAFRAME,
    DEFAULT_POINTS_PER_REQUEST,
    PolarsExportHandler,
    _get_exported_timestamp_channel,
)
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)

ONE_HOUR_NS = 3_600_000_000_000

# Timestamps round-trip as integer epoch nanoseconds so no precision is lost between export and
# re-upload, and the timestamp column re-reads as a plain integer.
_TIMESTAMP_TYPE: Literal["epoch_nanoseconds"] = "epoch_nanoseconds"


@dataclass(frozen=True)
class ChannelSyncOptions:
    """Configuration for a channel-data sync run.

    The window ``[start, end)`` is subdivided into ``bucket``-wide buckets for detection.
    ``tags`` is an optional datascope tag-filter applied on both the detection and the export, and
    carried verbatim on the re-uploaded points.
    """

    bucket: IntegralNanosecondsUTC = ONE_HOUR_NS
    tags: Mapping[str, str] | None = None
    max_retries: int = 2
    """How many times to re-stream a still-short range after the first attempt."""
    settle_seconds: float = 30.0
    """How long to wait for asynchronous ingestion to settle before re-detecting."""
    detect_workers: int = DEFAULT_DETECT_WORKERS
    """Threads issuing batched detection (count) requests concurrently."""
    detect_channels_per_request: int = DEFAULT_DETECT_CHANNELS_PER_REQUEST
    """Channels summarized per batched detection request (batch_compute_with_units)."""
    detect_request_delay: float = 0.0
    """Seconds to sleep between consecutive batch_compute_with_units submissions (rate-limiting)."""
    num_workers: int = DEFAULT_NUM_WORKERS
    """Worker threads for the export download pool."""
    batch_size: int = 50_000
    """Write-stream batch size."""
    points_per_request: int = DEFAULT_POINTS_PER_REQUEST
    """Export tuning: target points per export request (per channel group / time batch)."""
    points_per_dataframe: int = DEFAULT_POINTS_PER_DATAFRAME
    """Export tuning: target points per written file; drives automatic time-batching."""
    channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST
    """Export tuning: max channels per export request (column-partitions large channel sets)."""
    max_concurrent_links: int = DEFAULT_MAX_CONCURRENT_LINKS
    """Export tuning: max presigned links generated concurrently (bounds backend compute queries)."""
    show_progress: bool = True
    """Render a single determinate progress bar for the whole pass, measured in slices (channel x
    missing-bucket units, the total known up front from detection). Route logs to a file when
    enabled, since the live display and interleaved log lines on stdout corrupt each other."""
    output_dir: Path | None = None
    """Directory for exported CSVs; a temporary directory is used (and cleaned up) when omitted.
    Required when ``phase`` is ``"download"`` or ``"stream"`` (those phases must persist/read files)."""
    phase: Literal["all", "plan", "download", "stream"] = "all"
    """Which stage(s) to run. ``"all"`` (default) detects, downloads+streams (pipelined), then settles
    and re-detects/retries. ``"plan"`` only detects and reports what would sync. ``"download"`` detects
    and exports the missing ranges to ``output_dir`` without streaming (files are kept). ``"stream"``
    skips detection/export and streams every CSV already in ``output_dir`` into the destination. The
    single-stage phases run once with no settle/retry loop, and compose across separate invocations."""


@dataclass(frozen=True)
class StillShort:
    """A (channel, tags, time-range) slice that remained short after all retries."""

    channel: str
    tags: dict[str, str]
    time_range: tuple[int, int]


@dataclass
class ChannelSyncReport:
    """Summary of what a sync run examined and moved."""

    channels_examined: int = 0
    channels_skipped_unsupported: int = 0
    channels_missing: int = 0
    """Channels that had at least one short bucket on the initial detection."""
    channels_synced: int = 0
    """Channels that started short and ended fully filled."""
    points_streamed: int = 0
    still_short: list[StillShort] = field(default_factory=list)
    planned_ranges: dict[str, list[tuple[int, int]]] = field(default_factory=dict)
    """For ``phase="plan"``: ``{channel_name: [(start, end), ...]}`` of the missing ranges that a full
    run would sync. Empty for the other phases."""


@contextlib.contextmanager
def _progress_bar(show: bool, total: int, description: str) -> Iterator[Callable[[int], None] | None]:
    """Yield an ``advance(n)`` callable rendering one determinate Rich bar, or ``None`` if not shown.

    Used for both the detection bars (counted in channels) and the streaming bar (counted in slices);
    ``total`` is known exactly up front in each case, so the bar shows a real percentage + ETA and
    fills to 100%. When there is nothing to count (``total <= 0`` -- e.g. detecting against a freshly
    empty destination), no bar is rendered rather than a misleading single phantom unit.
    """
    if not show or total <= 0:
        yield None
        return

    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        TaskProgressColumn,
        TextColumn,
        TimeRemainingColumn,
    )

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
    )
    with progress:
        task = progress.add_task(description, total=total)
        yield lambda n: progress.advance(task, n)


def sync_missing_channel_data(
    source_dataset: Dataset,
    source_client: NominalClient,
    destination_dataset: Dataset | None,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    options: ChannelSyncOptions | None = None,
) -> ChannelSyncReport:
    """Sync the channel data ``destination_dataset`` is missing relative to ``source_dataset``.

    ``source_client`` is the source tenant's client (used to drive the export). Returns a
    :class:`ChannelSyncReport`; channels/ranges that could not be filled are logged as warnings and
    listed in ``report.still_short`` rather than raising.

    ``destination_dataset`` may be ``None`` only for ``phase="plan"`` and ``phase="download"``: the
    destination is treated as empty (all source data in the window is considered missing). This lets
    you export and inspect data without configuring destination credentials.
    """
    options = options or ChannelSyncOptions()
    if options.phase in ("download", "stream") and options.output_dir is None:
        raise ValueError(f"phase={options.phase!r} requires output_dir (files must be persisted/read from disk)")
    if destination_dataset is None and options.phase in ("stream", "all"):
        raise ValueError(f"phase={options.phase!r} requires destination_dataset")
    report = ChannelSyncReport()

    all_channels = list(source_dataset.search_channels())
    report.channels_examined = len(all_channels)
    source_channels = [ch for ch in all_channels if ch.data_type in _EXPORTABLE_DATA_TYPES]
    report.channels_skipped_unsupported = len(all_channels) - len(source_channels)
    if report.channels_skipped_unsupported:
        logger.warning(
            "Skipping %d non-exportable channel(s) (only DOUBLE/INT/STRING are synced)",
            report.channels_skipped_unsupported,
        )
    if not source_channels:
        logger.info("No exportable channels found on source dataset %s", source_dataset.rid)
        return report

    source_by_name = {ch.name: ch for ch in source_channels}
    type_by_name: dict[str, ChannelDataType] = {
        ch.name: dt for ch in source_channels if (dt := ch.data_type) is not None
    }

    # phase="stream": don't detect or export -- just push whatever CSVs are already on disk. Source
    # channels are listed only to know each column's data type (and tags/bucket) for the re-read.
    if options.phase == "stream":
        assert options.output_dir is not None  # guaranteed by the phase validation above
        assert destination_dataset is not None  # guaranteed by the phase validation above
        logger.info("Streaming pre-downloaded files from %s into the destination", options.output_dir)
        report.points_streamed = _stream_from_dir(destination_dataset, options.output_dir, type_by_name, options)
        return report

    # Source counts never change across retries -- compute them once, in batch. Always running source
    # detection (even when destination is None) filters out channels with no data in the window,
    # avoiding wasted export requests for empty channels.
    logger.info("Counting source data across %d channel(s)", len(source_channels))
    with _progress_bar(options.show_progress, len(source_channels), "Counting source channels") as advance:
        source_counts = count_channels(
            source_channels,
            start,
            end,
            options.bucket,
            options.tags,
            channels_per_request=options.detect_channels_per_request,
            workers=options.detect_workers,
            request_delay=options.detect_request_delay,
            on_advance=advance,
        )

    # Channels detection could only presence-probe (precise=False) are the ones whose rate the export
    # can't estimate (e.g. high-cardinality enums that error with Compute:TooManyCategories). They get
    # the per-channel recursive-halving export fallback instead of the rate-sized grouped path.
    non_precise = {name for name, counts in source_counts.items() if not counts.precise}
    # When destination is None, _detect_missing treats it as empty: any source bucket with data > 0
    # is flagged as missing, so channels with zero source data are silently skipped.
    missing = _detect_missing(source_counts, destination_dataset, start, end, options)

    report.channels_missing = len(missing)
    if not missing:
        logger.info("Destination is already complete over the window; nothing to sync")
        return report

    # phase="plan": report what a full run would sync and stop before touching the destination.
    if options.phase == "plan":
        report.planned_ranges = {name: list(ranges) for name, ranges in missing.items()}
        total_ranges = sum(len(ranges) for ranges in missing.values())
        logger.info("Plan: %d channel(s) short across %d range(s)", len(missing), total_ranges)
        tags_label = f" tags={dict(options.tags)}" if options.tags else ""
        for name, ranges in missing.items():
            logger.info("  %s%s: %s", name, tags_label, [(rs, re) for rs, re in ranges])
        return report

    handler = PolarsExportHandler(
        source_client,
        points_per_request=options.points_per_request,
        points_per_dataframe=options.points_per_dataframe,
        channels_per_request=options.channels_per_request,
        num_workers=options.num_workers,
        max_concurrent_links=options.max_concurrent_links,
    )

    # phase="download": export the missing ranges to output_dir without streaming, then stop. A later
    # phase="stream" (or phase="all", which reuses size-matched files) ingests them.
    if options.phase == "download":
        logger.info("Downloading %d channel(s) with missing data to %s", len(missing), options.output_dir)
        _stream_missing(handler, destination_dataset, missing, source_by_name, non_precise, options, download_only=True)
        return report

    # phase="all": stream, settle, re-detect, and retry whatever is still short.
    _sync_and_retry(
        handler, destination_dataset, missing, source_by_name, non_precise, source_counts, start, end, options, report
    )
    return report


def _sync_and_retry(
    handler: PolarsExportHandler,
    destination_dataset: Dataset,
    missing: dict[str, list[tuple[int, int]]],
    source_by_name: Mapping[str, Channel],
    non_precise: set[str],
    source_counts: Mapping[str, ChannelBucketCounts],
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    options: ChannelSyncOptions,
    report: ChannelSyncReport,
) -> None:
    """Stream the missing ranges, then settle + re-detect + re-stream until nothing is short or retries
    are exhausted. Mutates ``report`` (points_streamed, channels_synced, still_short).
    """
    for attempt in range(options.max_retries + 1):
        if attempt == 0:
            logger.info("Syncing %d channel(s) with missing data", len(missing))
        else:
            logger.info("Retry %d/%d: %d channel(s) still short", attempt, options.max_retries, len(missing))

        report.points_streamed += _stream_missing(
            handler, destination_dataset, missing, source_by_name, non_precise, options
        )

        # Streaming ingestion is eventually-consistent; let it settle before re-counting so we don't
        # report false shortfalls (and needlessly re-stream).
        if options.settle_seconds > 0:
            logger.debug("Waiting %.1fs for ingestion to settle", options.settle_seconds)
            time.sleep(options.settle_seconds)

        retry_channels = [source_by_name[name] for name in missing]
        missing = _detect_missing(source_counts, destination_dataset, start, end, options, only=retry_channels)
        if not missing:
            break

    report.channels_synced = report.channels_missing - len(missing)
    base_tags = dict(options.tags or {})
    for name, ranges in missing.items():
        for time_range in ranges:
            logger.warning(
                "Channel %r tags=%s range=[%d, %d) is still short after %d attempt(s); not synced",
                name,
                base_tags,
                time_range[0],
                time_range[1],
                options.max_retries + 1,
            )
            report.still_short.append(StillShort(name, base_tags, time_range))


def _detect_missing(
    source_counts: Mapping[str, ChannelBucketCounts],
    destination_dataset: Dataset | None,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    options: ChannelSyncOptions,
    only: Sequence[Channel] | None = None,
) -> dict[str, list[tuple[int, int]]]:
    """Return ``{channel_name: missing_ranges}`` for channels short in the destination.

    ``only`` restricts the comparison to the given channels (used on retries); otherwise every
    channel in ``source_counts`` is checked. Destination channels are looked up fresh each call so
    a re-detect sees newly-streamed data, and counted in batch.

    When ``destination_dataset`` is ``None``, the destination is treated as empty: every source
    bucket is considered missing.
    """
    scope_names = {ch.name for ch in only} if only is not None else set(source_counts)

    if destination_dataset is None:
        logger.info("No destination configured; treating destination as empty — only channels with source data will sync")
        dest_counts: Mapping[str, ChannelBucketCounts] = {}
    else:
        dest_by_name = {ch.name: ch for ch in destination_dataset.search_channels()}
        in_scope_dest = [dest_by_name[name] for name in scope_names if name in dest_by_name]

        logger.info(
            "Counting destination data across %d of %d in-scope channel(s)", len(in_scope_dest), len(scope_names)
        )
        with _progress_bar(options.show_progress, len(in_scope_dest), "Counting destination channels") as advance:
            dest_counts = count_channels(
                in_scope_dest,
                start,
                end,
                options.bucket,
                options.tags,
                channels_per_request=options.detect_channels_per_request,
                workers=options.detect_workers,
                request_delay=options.detect_request_delay,
                on_advance=advance,
            )

    missing: dict[str, list[tuple[int, int]]] = {}
    for name in scope_names:
        src = source_counts[name]
        # Channel absent in the destination (not in dest_counts) -> zero data everywhere.
        dest = dest_counts.get(name) or ChannelBucketCounts(name, {}, src.precise)
        short = shortfall_buckets(src, dest)
        if short:
            missing[name] = merge_bucket_ranges(short, options.bucket)
    return missing


def _buckets_in_range(range_start: int, range_end: int, bucket: IntegralNanosecondsUTC) -> int:
    """Number of ``bucket``-wide buckets in the (bucket-aligned) ``[range_start, range_end)``."""
    return (range_end - range_start) // int(bucket)


def _stream_missing(
    handler: PolarsExportHandler,
    destination_dataset: Dataset | None,
    missing: Mapping[str, list[tuple[int, int]]],
    source_by_name: Mapping[str, Channel],
    non_precise: set[str],
    options: ChannelSyncOptions,
    download_only: bool = False,
) -> int:
    """Export each channel's missing ranges from the source and stream them to the destination.

    Channels whose rate the export *can* estimate (``precise``) are grouped by identical missing-range
    signature and exported together. Channels detection could only presence-probe (``non_precise`` --
    e.g. high-cardinality enums the rate estimator rejects) are exported one channel at a time with a
    recursive-halving fallback (see :func:`_export_and_stream_channel`). Returns points streamed.

    When ``download_only`` is set, no write stream is opened: each exported file is downloaded to
    ``output_dir`` and kept (not streamed, not deleted). The progress bar still advances by the slices
    each downloaded file covers. The return value is the would-stream point count and is ignored.
    """
    # Source channels were filtered to exportable (non-None) data types upstream, so data_type is
    # always present here; the comprehension makes that explicit for the type checker.
    type_by_name: dict[str, ChannelDataType] = {
        name: dt for name in missing if (dt := source_by_name[name].data_type) is not None
    }

    # Group the rate-estimable channels by identical missing-range signature so each range is exported
    # once with all the channels that need exactly it.
    groups: dict[tuple[tuple[int, int], ...], list[Channel]] = {}
    for name, ranges in missing.items():
        if name not in non_precise:
            groups.setdefault(tuple(ranges), []).append(source_by_name[name])
    fallback = [(source_by_name[name], ranges) for name, ranges in missing.items() if name in non_precise]

    # A slice is one (channel, missing-bucket) unit -- the exact, up-front total for the progress bar.
    total_slices = sum(_buckets_in_range(rs, re, options.bucket) for ranges in missing.values() for rs, re in ranges)

    points = 0
    description = "Downloading slices" if download_only else "Syncing slices"
    # One progress display for the whole pass (the export's own per-call bars are disabled). The bar
    # advances per processed file -- by the slices that file covers -- so it moves smoothly and only
    # counts data that actually landed (failed exports don't advance it). In download-only mode no
    # write stream is opened; files are downloaded and kept rather than streamed.
    stream_ctx: contextlib.AbstractContextManager[DataStream | None] = (
        contextlib.nullcontext(None)
        if download_only
        else destination_dataset.get_write_stream(batch_size=options.batch_size)
    )
    with (
        _progress_bar(options.show_progress, total_slices, description) as advance,
        stream_ctx as stream,
    ):
        for signature, channels in groups.items():
            for range_start, range_end in signature:
                points += _export_and_stream_range(
                    handler, stream, channels, range_start, range_end, type_by_name, options, advance
                )
        for channel, ranges in fallback:
            for range_start, range_end in ranges:
                points += _export_and_stream_channel(
                    handler, stream, channel, range_start, range_end, type_by_name, options, advance
                )
    # Exiting the context flushes and closes the stream (wait=True).
    return points


def _export_and_stream(
    handler: PolarsExportHandler,
    stream: DataStream | None,
    channels: Sequence[Channel],
    range_start: int,
    range_end: int,
    type_by_name: Mapping[str, ChannelDataType],
    options: ChannelSyncOptions,
    advance: Callable[[int], None] | None,
    *,
    skip_rate_estimation: bool = False,
) -> int:
    """Export ``channels`` over ``[range_start, range_end)`` to CSV, streaming each file the instant it
    finishes downloading. Returns points streamed; **re-raises** any whole-export failure so callers can
    decide how to handle it. A single-file *streaming* failure is non-fatal (logged, range left short).

    ``on_file_complete`` fires serially from the export's download-driving thread (no locking needed);
    ``advance`` (when given) moves the shared bar by the slices each file covers as it streams. When
    ``stream`` is ``None`` (download-only), each file is downloaded and kept but not streamed; the bar
    still advances by the file's slices.
    """
    if options.output_dir is not None:
        options.output_dir.mkdir(parents=True, exist_ok=True)
    tmp_ctx: contextlib.AbstractContextManager[str] = (
        contextlib.nullcontext(str(options.output_dir))
        if options.output_dir is not None
        else tempfile.TemporaryDirectory()
    )
    # When exporting to a temporary directory, delete each file once streamed so peak disk stays at
    # ~one file instead of the whole batch. A caller-provided output_dir is left intact for inspection.
    cleanup_files = options.output_dir is None
    points = 0

    def _on_file_complete(path: Path) -> None:
        nonlocal points
        try:
            streamed, slices = _stream_file(stream, path, type_by_name, options.tags, options.bucket)
            points += streamed
            if advance is not None:
                advance(slices)
        except Exception:
            logger.exception("Failed to process exported file %s; range will be retried on re-detect", path)
        finally:
            if cleanup_files:
                path.unlink(missing_ok=True)

    with tmp_ctx as out_dir:
        handler.export_to_files(
            channels,
            range_start,
            range_end,
            out_dir,
            tags=options.tags,
            timestamp_type=_TIMESTAMP_TYPE,
            file_prefix=f"sync_{range_start}_{range_end}",
            show_progress=False,
            on_file_complete=_on_file_complete,
            # Reuse files a prior attempt/run already downloaded (size-matched) instead of failing to
            # re-create them; the retry then re-streams the existing file rather than colliding.
            reuse_complete=True,
            skip_rate_estimation=skip_rate_estimation,
        )
    return points


def _export_and_stream_range(
    handler: PolarsExportHandler,
    stream: DataStream | None,
    channels: Sequence[Channel],
    range_start: int,
    range_end: int,
    type_by_name: Mapping[str, ChannelDataType],
    options: ChannelSyncOptions,
    advance: Callable[[int], None] | None = None,
) -> int:
    """Grouped, rate-sized export of ``channels`` over a range (the normal path). A whole-export
    failure is non-fatal: logged, and the range is left short for the verify/re-detect loop to retry.
    """
    try:
        return _export_and_stream(handler, stream, channels, range_start, range_end, type_by_name, options, advance)
    except Exception as exc:
        logger.exception(
            "Export failed for range [%d, %d) over %d channel(s) (%s); range will be retried on re-detect",
            range_start,
            range_end,
            len(channels),
            exc,
        )
        return 0


def _export_and_stream_channel(
    handler: PolarsExportHandler,
    stream: DataStream | None,
    channel: Channel,
    range_start: int,
    range_end: int,
    type_by_name: Mapping[str, ChannelDataType],
    options: ChannelSyncOptions,
    advance: Callable[[int], None] | None,
) -> int:
    """Export+stream one channel whose rate can't be estimated, halving the range and retrying on
    export failure.

    The export request size for these channels can't be planned (rate estimation fails), so we try the
    whole range and, if the export request fails (e.g. too large for the backend), recursively split
    the range at a bucket-aligned midpoint and retry each half -- discovering a workable size by trial.
    Bottoms out at one ``bucket``: a single-bucket export that still fails is genuinely unexportable and
    is left short (logged).
    """
    try:
        return _export_and_stream(
            handler,
            stream,
            [channel],
            range_start,
            range_end,
            type_by_name,
            options,
            advance,
            skip_rate_estimation=True,
        )
    except Exception as exc:
        span = range_end - range_start
        if span <= int(options.bucket):
            logger.warning(
                "Channel %r range [%d, %d) could not be exported even at one-bucket granularity (%s); leaving short",
                channel.name,
                range_start,
                range_end,
                exc,
            )
            return 0
        # Split at a bucket-aligned midpoint; guarantee forward progress.
        mid = range_start + max(1, (span // int(options.bucket)) // 2) * int(options.bucket)
        logger.info(
            "Export failed for channel %r over [%d, %d) (%s); halving at %d and retrying",
            channel.name,
            range_start,
            range_end,
            exc,
            mid,
        )
        return _export_and_stream_channel(
            handler, stream, channel, range_start, mid, type_by_name, options, advance
        ) + _export_and_stream_channel(handler, stream, channel, mid, range_end, type_by_name, options, advance)


def _stream_from_dir(
    destination_dataset: Dataset,
    output_dir: Path,
    type_by_name: Mapping[str, ChannelDataType],
    options: ChannelSyncOptions,
) -> int:
    """Stream every exported CSV already in ``output_dir`` into the destination (the ``"stream"`` phase).

    This is the read-from-disk counterpart of the download phase: no detection or export runs, so the
    files are taken as-is and each is fed through :func:`_stream_file`. There is no detection plan to
    size the bar in slices, so it advances per file. Files are left on disk. Returns points streamed.
    """
    files = sorted(p for p in output_dir.glob("*.csv*") if p.is_file())
    if not files:
        logger.warning("No exported CSVs found in %s; nothing to stream", output_dir)
        return 0

    logger.info("Streaming %d file(s) from %s", len(files), output_dir)
    points = 0
    with (
        _progress_bar(options.show_progress, len(files), "Streaming files") as advance,
        destination_dataset.get_write_stream(batch_size=options.batch_size) as stream,
    ):
        for path in files:
            try:
                streamed, _ = _stream_file(stream, path, type_by_name, options.tags, options.bucket)
                points += streamed
            except Exception:
                logger.exception("Failed to stream file %s; skipping", path)
            finally:
                if advance is not None:
                    advance(1)
    # Exiting the context flushes and closes the stream (wait=True).
    return points


def _polars_dtype(data_type: ChannelDataType) -> pl.DataType:
    """Map a channel data type to the polars dtype used to force the CSV re-read schema.

    STRING channels are read as strings (so numeric-looking labels stay strings). All numeric
    channels -- including ``INT`` -- are read as ``Float64``: a channel's declared type cannot be
    trusted to match its exported values (an ``INT``-typed channel may export floats), and forcing
    ``Int64`` would fail to parse such a file. Forcing ``Float64`` also keeps a ``DOUBLE`` channel
    whose values happen to look integral from being inferred (and re-created in the destination) as
    an integer. Genuinely-integral ``INT`` values are re-cast back to ``int`` at enqueue time.
    """
    if data_type == ChannelDataType.STRING:
        return pl.String()
    return pl.Float64()


def _stream_file(
    stream: DataStream | None,
    path: Path,
    type_by_name: Mapping[str, ChannelDataType],
    tags: Mapping[str, str] | None,
    bucket: IntegralNanosecondsUTC,
) -> tuple[int, int]:
    """Re-read one exported gzipped CSV and enqueue every non-null point into ``stream``.

    The file is a wide CSV: one timestamp column (epoch nanoseconds) plus one column per channel.
    The timestamp column is the single column that is not a known channel name. Channel columns are
    parsed with their source data type so values stream up with the correct type.

    When ``stream`` is ``None`` the file is read and measured but not enqueued (download-only mode):
    the returned point count reflects what *would* stream, and the slice count still advances the bar.

    Returns ``(points_streamed, slices_covered)`` where a slice is one (channel, ``bucket``-wide
    bucket) cell that had data in this file -- used to advance the per-file progress bar. Each
    (channel, bucket) lives in exactly one file, so per-file slice counts sum to the run's total.
    """
    # Peek the header to know which columns this column-partitioned file carries, then force the
    # schema for the channel columns (timestamp infers as Int64 from epoch-nanosecond integers).
    with gzip.open(path, "rb") as fh:
        raw = fh.read()
    # infer_schema_length=0 reads every column as a string -- we only need the names here, and this
    # peek must NOT infer types: a numeric column that looks integral within the inference sample but
    # holds a float later would otherwise raise "could not parse '<float>' as dtype i64" *before* the
    # type-forced main read below ever runs.
    header = pl.read_csv(raw, n_rows=0, infer_schema_length=0).columns
    schema_overrides = {col: _polars_dtype(type_by_name[col]) for col in header if col in type_by_name}
    # Wide multi-channel exports merge on timestamp, so a channel without a sample at a given row has
    # an empty cell. Treat empty as null (not the string "") so those rows are dropped, not streamed.
    # infer_schema_length=None scans the whole file when inferring any column not covered by the
    # overrides above, so a numeric column whose first rows look integral can't be typed Int64 and
    # then fail on a later float ("could not parse '12.65' as dtype i64").
    frame = pl.read_csv(raw, schema_overrides=schema_overrides, null_values=[""], infer_schema_length=None)
    if frame.height == 0:
        return 0, 0

    data_columns = [col for col in frame.columns if col in type_by_name]
    time_candidates = [col for col in frame.columns if col not in type_by_name]
    if len(time_candidates) == 1:
        time_col = time_candidates[0]
    else:
        # Fallback: a channel literally named "timestamp" can confuse the heuristic above.
        time_col = _get_exported_timestamp_channel(data_columns)

    points = 0
    slices = 0
    for channel_name in data_columns:
        column = frame.select([time_col, channel_name]).drop_nulls(channel_name)
        if column.height == 0:
            continue
        timestamps = column.get_column(time_col).to_list()
        values = column.get_column(channel_name).to_list()
        # INT channels were read as Float64 (see _polars_dtype); re-cast whole-number values back to
        # int so a genuine integer channel streams as INT, while non-integral values stay float.
        if type_by_name[channel_name] == ChannelDataType.INT:
            values = [int(v) if isinstance(v, float) and v.is_integer() else v for v in values]
        if stream is not None:
            stream.enqueue_batch(channel_name, timestamps, values, tags)
        points += len(values)
        # Distinct buckets this channel has data in within this file -- one slice each.
        slices += (column.get_column(time_col) // int(bucket)).n_unique()
    return points, slices


_TAGS_METADATA_FILE = "sync_tags.json"


def sync_missing_channel_data_for_tag_filters(
    source_dataset: Dataset,
    source_client: NominalClient,
    destination_dataset: Dataset | None,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    tag_filters: Sequence[Mapping[str, str]] | None = None,
    base_options: ChannelSyncOptions | None = None,
) -> list[ChannelSyncReport]:
    """Run :func:`sync_missing_channel_data` once per tag filter, returning one report per filter.

    ``tag_filters`` is a list of tag dicts (e.g. ``[{"asset_id": "cr230"}, {"asset_id": "cr236"}]``).
    Each filter drives a separate sync pass over the same ``[start, end)`` window.

    When ``base_options.output_dir`` is set, each filter's exported files land in a subdirectory
    named after the filter (e.g. ``output_dir/asset_id_cr236/``), and a ``sync_tags.json`` file is
    written there so the stream phase can reconstruct the tag mapping without re-specifying it.

    For ``phase="stream"``, ``tag_filters`` may be omitted: the function auto-discovers subdirectories
    under ``base_options.output_dir`` that contain a ``sync_tags.json`` written by a prior download.
    """
    import json

    base_options = base_options or ChannelSyncOptions()

    if tag_filters is None:
        if base_options.phase != "stream" or base_options.output_dir is None:
            raise ValueError("tag_filters is required unless phase='stream' and output_dir is set (auto-discovery)")
        tag_filters = _discover_tag_filters(base_options.output_dir)
        if not tag_filters:
            logger.warning("No %s files found under %s; nothing to stream", _TAGS_METADATA_FILE, base_options.output_dir)
            return []

    reports = []
    for tags in tag_filters:
        tag_label = "_".join(f"{k}_{v}" for k, v in tags.items())
        output_dir = base_options.output_dir / tag_label if base_options.output_dir is not None else None
        options = replace(base_options, tags=tags, output_dir=output_dir)
        logger.info("=== Syncing tag filter: %s ===", tag_label)

        if base_options.phase == "download" and output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / _TAGS_METADATA_FILE).write_text(json.dumps(dict(tags)))

        report = sync_missing_channel_data(source_dataset, source_client, destination_dataset, start, end, options)
        reports.append(report)
    return reports


def _discover_tag_filters(output_dir: Path) -> list[dict[str, str]]:
    """Return tag dicts from ``sync_tags.json`` files in subdirectories of ``output_dir``."""
    import json

    filters = []
    for subdir in sorted(output_dir.iterdir()):
        meta = subdir / _TAGS_METADATA_FILE
        if subdir.is_dir() and meta.exists():
            filters.append(json.loads(meta.read_text()))
            logger.debug("Discovered tag filter %s from %s", filters[-1], meta)
    return filters
