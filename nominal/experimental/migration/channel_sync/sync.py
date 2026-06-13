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
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
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
    """Render a single live progress display for the whole pass (links prepared, files downloaded,
    points streamed, ranges done). Route logs to a file when enabled, since the live display and
    interleaved log lines on stdout corrupt each other."""
    output_dir: Path | None = None
    """Directory for exported CSVs; a temporary directory is used (and cleaned up) when omitted."""


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


class _SyncProgress:
    """Thin wrapper over a Rich progress display shared across every export call in one pass.

    Four bars: ranges done (determinate), links prepared, files downloaded, and points streamed
    (running counters). Updates come serially from the export download-driving thread and the main
    loop; Rich's progress is itself thread-safe.
    """

    def __init__(self, progress: object, ranges: int, prepared: int, downloaded: int, streamed: int) -> None:
        self._progress = progress
        self._ranges = ranges
        self._prepared = prepared
        self._downloaded = downloaded
        self._streamed = streamed

    def range_done(self) -> None:
        self._progress.advance(self._ranges, 1)  # type: ignore[attr-defined]

    def file_prepared(self) -> None:
        self._progress.advance(self._prepared, 1)  # type: ignore[attr-defined]

    def file_downloaded(self) -> None:
        self._progress.advance(self._downloaded, 1)  # type: ignore[attr-defined]

    def points_streamed(self, n: int) -> None:
        self._progress.advance(self._streamed, n)  # type: ignore[attr-defined]


@contextlib.contextmanager
def _sync_progress(show: bool, total_ranges: int) -> Iterator[_SyncProgress | None]:
    """Yield a :class:`_SyncProgress` rendering one live display, or ``None`` when ``show`` is False.

    The "ranges" bar is determinate (total known up front); links/files/points are running counters
    (their totals are only known as the export plans, so they show a count rather than a percentage).
    """
    if not show:
        yield None
        return

    from rich.progress import BarColumn, Progress, ProgressColumn, TextColumn, TimeElapsedColumn
    from rich.text import Text

    class _CountColumn(ProgressColumn):
        def render(self, task: object) -> Text:
            completed = int(getattr(task, "completed", 0))
            total = getattr(task, "total", None)
            return Text(f"{completed:,}" if total is None else f"{completed:,}/{int(total):,}")

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        _CountColumn(),
        TimeElapsedColumn(),
    )
    with progress:
        ranges = progress.add_task("Ranges synced", total=total_ranges)
        prepared = progress.add_task("Links prepared", total=None)
        downloaded = progress.add_task("Files downloaded", total=None)
        streamed = progress.add_task("Points streamed", total=None)
        yield _SyncProgress(progress, ranges, prepared, downloaded, streamed)


def sync_missing_channel_data(
    source_dataset: Dataset,
    source_client: NominalClient,
    destination_dataset: Dataset,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    options: ChannelSyncOptions | None = None,
) -> ChannelSyncReport:
    """Sync the channel data ``destination_dataset`` is missing relative to ``source_dataset``.

    ``source_client`` is the source tenant's client (used to drive the export). Returns a
    :class:`ChannelSyncReport`; channels/ranges that could not be filled are logged as warnings and
    listed in ``report.still_short`` rather than raising.
    """
    options = options or ChannelSyncOptions()
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
    # Source counts never change across retries -- compute them once, in batch.
    logger.info("Counting source data across %d channel(s)", len(source_channels))
    source_counts = count_channels(
        source_channels,
        start,
        end,
        options.bucket,
        options.tags,
        channels_per_request=options.detect_channels_per_request,
        workers=options.detect_workers,
    )

    missing = _detect_missing(source_counts, destination_dataset, start, end, options)
    report.channels_missing = len(missing)
    if not missing:
        logger.info("Destination is already complete over the window; nothing to sync")
        return report

    handler = PolarsExportHandler(
        source_client,
        points_per_request=options.points_per_request,
        points_per_dataframe=options.points_per_dataframe,
        channels_per_request=options.channels_per_request,
        num_workers=options.num_workers,
        max_concurrent_links=options.max_concurrent_links,
    )
    for attempt in range(options.max_retries + 1):
        if attempt == 0:
            logger.info("Syncing %d channel(s) with missing data", len(missing))
        else:
            logger.info("Retry %d/%d: %d channel(s) still short", attempt, options.max_retries, len(missing))

        report.points_streamed += _stream_missing(handler, destination_dataset, missing, source_by_name, options)

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

    return report


def _detect_missing(
    source_counts: Mapping[str, ChannelBucketCounts],
    destination_dataset: Dataset,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    options: ChannelSyncOptions,
    only: Sequence[Channel] | None = None,
) -> dict[str, list[tuple[int, int]]]:
    """Return ``{channel_name: missing_ranges}`` for channels short in the destination.

    ``only`` restricts the comparison to the given channels (used on retries); otherwise every
    channel in ``source_counts`` is checked. Destination channels are looked up fresh each call so
    a re-detect sees newly-streamed data, and counted in batch.
    """
    scope_names = {ch.name for ch in only} if only is not None else set(source_counts)
    dest_by_name = {ch.name: ch for ch in destination_dataset.search_channels()}
    in_scope_dest = [dest_by_name[name] for name in scope_names if name in dest_by_name]

    logger.info("Counting destination data across %d of %d in-scope channel(s)", len(in_scope_dest), len(scope_names))
    dest_counts = count_channels(
        in_scope_dest,
        start,
        end,
        options.bucket,
        options.tags,
        channels_per_request=options.detect_channels_per_request,
        workers=options.detect_workers,
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


def _stream_missing(
    handler: PolarsExportHandler,
    destination_dataset: Dataset,
    missing: Mapping[str, list[tuple[int, int]]],
    source_by_name: Mapping[str, Channel],
    options: ChannelSyncOptions,
) -> int:
    """Export each channel's missing ranges from the source and stream them to the destination.

    Channels sharing an identical set of missing ranges are exported together (one
    ``export_to_files`` call per shared range). Returns the number of points streamed.
    """
    # Source channels were filtered to exportable (non-None) data types upstream, so data_type is
    # always present here; the comprehension makes that explicit for the type checker.
    type_by_name: dict[str, ChannelDataType] = {
        name: dt for name in missing if (dt := source_by_name[name].data_type) is not None
    }

    # Group channels by identical missing-range signature so each range is exported once with all
    # the channels that need exactly it -- no channel is exported over a range it isn't missing.
    groups: dict[tuple[tuple[int, int], ...], list[Channel]] = {}
    for name, ranges in missing.items():
        groups.setdefault(tuple(ranges), []).append(source_by_name[name])

    total_ranges = sum(len(signature) for signature in groups)
    points = 0
    # One progress display for the whole pass -- the export's own per-call bars are disabled so
    # downloads and streaming advance on shared bars instead of a fresh fragment per export call.
    with (
        _sync_progress(options.show_progress, total_ranges) as progress,
        destination_dataset.get_write_stream(batch_size=options.batch_size) as stream,
    ):
        for signature, channels in groups.items():
            for range_start, range_end in signature:
                points += _export_and_stream_range(
                    handler, stream, channels, range_start, range_end, type_by_name, options, progress
                )
                if progress is not None:
                    progress.range_done()
    # Exiting the context flushes and closes the stream (wait=True).
    return points


def _export_and_stream_range(
    handler: PolarsExportHandler,
    stream: DataStream,
    channels: Sequence[Channel],
    range_start: int,
    range_end: int,
    type_by_name: Mapping[str, ChannelDataType],
    options: ChannelSyncOptions,
    progress: _SyncProgress | None = None,
) -> int:
    """Export ``channels`` over ``[range_start, range_end)`` to CSV, streaming each file up the
    instant it finishes downloading (rather than waiting for the whole batch).

    The ``on_file_*`` hooks fire serially from the export's download-driving thread, so the streaming
    and progress updates below need no locking. A streaming failure for one file is non-fatal: it is
    logged and the range is left short, so the verify/re-detect loop retries it on the next pass.
    The handler's own progress bars are disabled (``show_progress=False``); ``progress`` (when given)
    is the shared display that spans the whole sync pass.
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

    def _on_file_planned(_path: Path) -> None:
        if progress is not None:
            progress.file_prepared()

    def _on_file_complete(path: Path) -> None:
        nonlocal points
        if progress is not None:
            progress.file_downloaded()
        try:
            streamed = _stream_file(stream, path, type_by_name, options.tags)
            points += streamed
            if progress is not None:
                progress.points_streamed(streamed)
        except Exception:
            logger.exception("Failed to stream exported file %s; range will be retried on re-detect", path)
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
            on_file_planned=_on_file_planned,
            on_file_complete=_on_file_complete,
        )
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
    stream: DataStream,
    path: Path,
    type_by_name: Mapping[str, ChannelDataType],
    tags: Mapping[str, str] | None,
) -> int:
    """Re-read one exported gzipped CSV and enqueue every non-null point into ``stream``.

    The file is a wide CSV: one timestamp column (epoch nanoseconds) plus one column per channel.
    The timestamp column is the single column that is not a known channel name. Channel columns are
    parsed with their source data type so values stream up with the correct type.
    """
    # Peek the header to know which columns this column-partitioned file carries, then force the
    # schema for the channel columns (timestamp infers as Int64 from epoch-nanosecond integers).
    with gzip.open(path, "rb") as fh:
        raw = fh.read()
    header = pl.read_csv(raw, n_rows=0).columns
    schema_overrides = {col: _polars_dtype(type_by_name[col]) for col in header if col in type_by_name}
    # Wide multi-channel exports merge on timestamp, so a channel without a sample at a given row has
    # an empty cell. Treat empty as null (not the string "") so those rows are dropped, not streamed.
    frame = pl.read_csv(raw, schema_overrides=schema_overrides, null_values=[""])
    if frame.height == 0:
        return 0

    data_columns = [col for col in frame.columns if col in type_by_name]
    time_candidates = [col for col in frame.columns if col not in type_by_name]
    if len(time_candidates) == 1:
        time_col = time_candidates[0]
    else:
        # Fallback: a channel literally named "timestamp" can confuse the heuristic above.
        time_col = _get_exported_timestamp_channel(data_columns)

    points = 0
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
        stream.enqueue_batch(channel_name, timestamps, values, tags)
        points += len(values)
    return points
