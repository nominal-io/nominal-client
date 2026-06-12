"""Export channel data to CSV files on disk via S3 presigned links + parallel multi-part downloads.

Unlike :class:`~nominal.thirdparty.polars.polars_export_handler.PolarsExportHandler`, which streams
CSV through the Nominal API server and merges everything into in-memory DataFrames, this handler
asks the export service for an S3 **presigned link** per request
(``generate_export_channel_data_presigned_link``) and downloads each file **straight to disk** using
parallel ranged GETs (:class:`MultipartFileDownloader`). This lifts the API-proxy size ceiling and is
much faster for large exports.

It inherits the shared, transport-agnostic planning from :class:`ExportHandler` (per-channel point
rates, channel grouping, time slicing, request building) and only changes how output is handled. Each
export request maps to one file, so an export of more than ``channels_per_request`` channels (or across
multiple datasources) produces several column-partitioned files covering the same timestamps. Use
:meth:`PresignedExportHandler.merge` to lazily reassemble them.
"""

from __future__ import annotations

import collections
import datetime
import logging
import pathlib
import time
import typing
from contextlib import contextmanager
from typing import Callable, Iterator, Mapping, Sequence

import requests
from nominal_api import scout_dataexport_api

if typing.TYPE_CHECKING:
    from rich.console import Console

import polars as pl
from nominal.core._utils.multipart import DEFAULT_CHUNK_SIZE
from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
)
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.client import NominalClient
from nominal.core.datasource import DataSource
from nominal.thirdparty.polars.export_handler import (
    DEFAULT_EXPORTED_TIMESTAMP_COL_NAME,
    MAX_NUM_BUCKETS,
    ExportHandler,
    _build_channel_groups,
    _channel_data_buckets,
    _channel_enum_buckets,
    _ExportJob,
    _group_channels_by_datatype,
    _max_rate_from_buckets,
    _TimeRange,
)
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    _AnyExportableTimestampType,
)

# Number of workers used across the API / download thread pool.
DEFAULT_PRESIGNED_NUM_WORKERS = 8

# Defaults are large: each presigned request becomes one file downloaded in parallel parts, so larger
# files make multi-part downloads worthwhile. With ~50 channels/request, 50M points is roughly 1M
# timestamps per request.
DEFAULT_PRESIGNED_POINTS_PER_REQUEST = 50_000_000
DEFAULT_PRESIGNED_POINTS_PER_FILE = 50_000_000
DEFAULT_PRESIGNED_CHANNELS_PER_REQUEST = 50

# Presigned links are long-lived; use a generous TTL so each link is fetched once and not proactively
# re-signed mid-download (re-signing may re-run the export). The downloader still re-signs on a 403.
DEFAULT_URL_TTL_SECS = 3600.0
DEFAULT_URL_SKEW_SECS = 60.0

# Large exports are materialized to S3 asynchronously: the presigned link is returned with the
# final `file_size_bytes` before the object is fully written, and the object's served size grows
# until complete. We poll the object until its served size reaches `file_size_bytes` before
# downloading, so we never capture a partially-written (header-only) file.
DEFAULT_READINESS_TIMEOUT_SECS = 600.0

logger = logging.getLogger(__name__)


@contextmanager
def _progress_bars(
    total: int, show: bool, console: Console | None = None
) -> Iterator[tuple[Callable[[], None], Callable[[], None]]]:
    """Yield ``(advance_prepare, advance_download)`` callables.

    When ``show`` is set, both advance tasks on a single Rich progress display: one tracks files
    whose presigned link is ready (planning/preallocation) and one tracks completed downloads. When
    not shown, both are no-ops. Passing the ``console`` shared with a Rich logging handler lets logs
    render cleanly above the live bars instead of corrupting them.
    """
    if not show:
        noop: Callable[[], None] = lambda: None  # noqa: E731 - trivial no-op
        yield noop, noop
        return

    from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        prepare = progress.add_task("Preparing links", total=total)
        download = progress.add_task("Downloading", total=total)
        yield (lambda: progress.advance(prepare, 1), lambda: progress.advance(download, 1))


def _is_transient_error(exc: Exception) -> bool:
    """True for errors worth retrying: HTTP 5xx/429 (incl. ConjureHTTPError) or network-level errors."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if isinstance(status, int):
        return status == 429 or status >= 500
    return isinstance(exc, requests.RequestException)


class PresignedExportHandler(ExportHandler):
    """Export channel data to CSV files on disk using S3 presigned links and multi-part downloads."""

    def __init__(
        self,
        client: NominalClient,
        *,
        points_per_request: int = DEFAULT_PRESIGNED_POINTS_PER_REQUEST,
        points_per_file: int = DEFAULT_PRESIGNED_POINTS_PER_FILE,
        channels_per_request: int = DEFAULT_PRESIGNED_CHANNELS_PER_REQUEST,
        num_workers: int = DEFAULT_PRESIGNED_NUM_WORKERS,
        part_size: int = DEFAULT_CHUNK_SIZE,
        url_ttl_secs: float = DEFAULT_URL_TTL_SECS,
        url_skew_secs: float = DEFAULT_URL_SKEW_SECS,
        max_part_retries: int = 3,
        max_link_retries: int = 3,
        timeout: float = 30.0,
        readiness_timeout: float = DEFAULT_READINESS_TIMEOUT_SECS,
    ):
        """Initialize the presigned CSV export handler.

        Args:
            client: Nominal client for communicating with the API.
            points_per_request: Target maximum number of points within a single export request (and
                thus a single file).
            points_per_file: Target maximum number of points within each written file; drives how the
                time range is subdivided into batches.
            channels_per_request: Maximum number of channels packed into a single request / file.
            num_workers: Number of parallel workers used for ranged downloads.
            part_size: Byte size of each ranged download part.
            url_ttl_secs: How long a fetched presigned URL is considered valid before refresh.
            url_skew_secs: Safety buffer subtracted from ``url_ttl_secs``.
            max_part_retries: Maximum retries per download part (IO, presigned expiry, etc.).
            max_link_retries: Maximum attempts to generate a presigned link per file, retried with
                backoff on transient errors (5xx / 429 / network) since each file's link is a
                separate server-side export request.
            timeout: Per-request connection timeout, in seconds.
            readiness_timeout: Maximum seconds to wait for a large export's S3 object to be fully
                materialized (served size == file_size_bytes) before downloading it.
        """
        super().__init__(
            client,
            points_per_request=points_per_request,
            points_per_dataframe=points_per_file,  # file size drives time-slice sizing
            channels_per_request=channels_per_request,
            num_workers=num_workers,
            compression=None,  # uncompressed .csv keeps merge() fully lazy
        )
        self._part_size = part_size
        self._url_ttl_secs = url_ttl_secs
        self._url_skew_secs = url_skew_secs
        self._max_part_retries = max_part_retries
        self._max_link_retries = max_link_retries
        self._timeout = timeout
        self._readiness_timeout = readiness_timeout

    def export(
        self,
        channels: Sequence[Channel],
        start: IntegralNanosecondsUTC,
        end: IntegralNanosecondsUTC,
        output_dir: str | pathlib.Path,
        *,
        tags: Mapping[str, str] | None = None,
        batch_duration: datetime.timedelta | None = None,
        timestamp_type: _AnyExportableTimestampType = "epoch_seconds",
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        file_prefix: str = "export",
        show_progress: bool = False,
        console: Console | None = None,
    ) -> list[pathlib.Path]:
        """Export the given channels to CSV files in ``output_dir`` and return the written paths.

        Each export request becomes one file; exports of more than ``channels_per_request`` channels
        (or spanning multiple datasources) produce several column-partitioned files that share
        timestamps. Reassemble them with :meth:`merge`.

        Args:
            channels: Channels to export.
            start: Start of the export range, in nanoseconds since the Unix epoch (UTC).
            end: End of the export range, in nanoseconds since the Unix epoch (UTC).
            output_dir: Directory to write files into. Created if it does not exist.
            tags: Key-value pairs used to filter channel data.
            batch_duration: Optional explicit batch duration; otherwise computed from point rates.
            timestamp_type: Timestamp format for the exported timestamp column.
            buckets: Optional decimation by number of buckets (mutually exclusive with ``resolution``).
            resolution: Optional decimation resolution in ns (mutually exclusive with ``buckets``).
            file_prefix: Prefix for written file names.
            show_progress: When True, render a Rich progress display with two bars -- one tracking
                files whose presigned link is ready, one tracking completed downloads. Do not enable
                while another Rich live display is already active.
            console: Optional Rich console to render the progress display on. Pass the same console
                your logging handler uses so log lines render cleanly above the live bars instead of
                corrupting them.

        Returns:
            The list of written file paths, sorted.
        """
        output_dir = pathlib.Path(output_dir)
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return []
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")
        tags = dict(tags or {})

        batch_duration = self._clamp_batch_duration_for_resolution(batch_duration, resolution)

        # Plan first (the only blocking step); rate estimation is internally multi-threaded.
        export_jobs = self._compute_export_jobs(
            channels, _TimeRange(start, end), timestamp_type, tags, buckets, resolution, batch_duration
        )
        items = self._build_download_items(export_jobs, output_dir, file_prefix)
        if not items:
            logger.warning("No export jobs computed-- returning")
            return []

        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Exporting %d channels into %d file(s) under %s", len(channels), len(items), output_dir)
        with (
            _progress_bars(len(items), show_progress, console) as (advance_prepare, advance_download),
            MultipartFileDownloader.create(
                max_workers=self._num_workers,
                timeout=self._timeout,
                max_part_retries=self._max_part_retries,
                header_provider=self._client._clients.header_provider,
            ) as downloader,
        ):
            results = downloader.download_files_pipelined(
                items,
                on_file_planned=lambda _path: advance_prepare(),
                on_file_complete=lambda _path: advance_download(),
            )

        if results.failed:
            for dest, ex in results.failed.items():
                logger.error("Failed to export %s", dest, exc_info=ex)
            raise RuntimeError(f"Failed to export {len(results.failed)} of {len(items)} file(s)")

        return sorted(results.succeeded)

    def _clamp_batch_duration_for_resolution(
        self, batch_duration: datetime.timedelta | None, resolution: IntegralNanosecondsDuration | None
    ) -> datetime.timedelta | None:
        """Clamp batch_duration so decimated exports stay within the backend bucket limit."""
        if resolution is None:
            return batch_duration

        computed = datetime.timedelta(seconds=(resolution * MAX_NUM_BUCKETS) / 1e9)
        if batch_duration is None:
            logger.info(
                "Manually setting batch_duration to %fs (resolution=%dns)", computed.total_seconds(), resolution
            )
            return computed
        elif computed < batch_duration:
            logger.warning(
                "Configured batch_duration of %fs would result in failing exports with resolution=%dns. "
                "Setting batch_duration to %fs instead.",
                batch_duration.total_seconds(),
                resolution,
                computed.total_seconds(),
            )
            return computed
        return batch_duration

    def _compute_export_jobs(
        self,
        channels: Sequence[Channel],
        time_range: _TimeRange,
        timestamp_type: _AnyExportableTimestampType,
        tags: Mapping[str, str] | None = None,
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        batch_duration: datetime.timedelta | None = None,
    ) -> Mapping[_TimeRange, Sequence[_ExportJob]]:
        """Plan export jobs, pruning numeric channels with no data in a given time slice.

        Overrides :meth:`ExportHandler._compute_export_jobs`: it runs a single bucketed-compute pass
        (:func:`_channel_data_buckets`) to learn both each channel's rate *and* which buckets contain
        data, then re-groups the present numeric channels per time slice so we never request (and
        download) a numeric channel for a slice where it has no data. Empty enum-channel handling is
        unchanged (string channels have no bucket presence to prune on).
        """
        if buckets is not None and resolution is not None:
            raise ValueError("Cannot provide `buckets` and `resolution`")

        partitioned = _group_channels_by_datatype(channels)
        enum_channels = partitioned.get(ChannelDataType.STRING, [])
        numeric_channels = [
            *partitioned.get(ChannelDataType.DOUBLE, []),
            *partitioned.get(ChannelDataType.INT, []),
        ]
        if batch_duration is None and not numeric_channels:
            raise ValueError("If no numeric channels are provided, a `batch_duration` must be provided!")
        unknown = partitioned.get(ChannelDataType.UNKNOWN, [])
        if unknown:
            logger.warning("Could not determine datatypes of %d channels-- ignoring for export", len(unknown))

        rates, presence = self._rates_and_presence(numeric_channels, time_range, tags)
        enum_presence, enum_undetermined = self._enum_presence(enum_channels, time_range, tags)

        batch_duration_ns = self._compute_batch_duration(batch_duration, enum_channels, time_range, rates)
        time_slices = time_range.subdivide_ns(batch_duration_ns)
        batch_timedelta = datetime.timedelta(seconds=batch_duration_ns / 1e9)
        channels_by_name = {c.name: c for c in channels}

        names_by_datasource: dict[str, set[str]] = collections.defaultdict(set)
        for group in (numeric_channels, enum_channels):
            for channel in group:
                names_by_datasource[channel.data_source].add(channel.name)

        jobs: dict[_TimeRange, list[_ExportJob]] = collections.defaultdict(list)
        for datasource_rid, names in names_by_datasource.items():
            ds_enum = [c for c in enum_channels if c.name in names]
            ds_numeric_names = [c.name for c in numeric_channels if c.name in names and c.name in rates]
            for time_slice in time_slices:
                present = [n for n in ds_numeric_names if self._slice_has_data(presence.get(n), time_slice)]
                channel_groups, large_channels = _build_channel_groups(
                    {n: rates[n] for n in present},
                    {n: channels_by_name[n] for n in present},
                    self._points_per_request,
                    self._channels_per_request,
                    batch_timedelta,
                )
                # Enum channels: prune those with no data in this slice; channels whose presence could
                # not be determined (e.g. too many categories) are exported for all slices.
                present_enum = [
                    c
                    for c in ds_enum
                    if c.name in enum_undetermined or self._slice_has_data(enum_presence.get(c.name), time_slice)
                ]
                channel_groups.extend([[c] for c in present_enum])

                for group in channel_groups:
                    if group:
                        jobs[time_slice].append(
                            self._make_job(datasource_rid, group, time_slice, tags, buckets, resolution, timestamp_type)
                        )

                # Large channels (present in this slice) are subdivided into per-request sub-slices.
                for channel in large_channels:
                    sub_offset = datetime.timedelta(seconds=self._points_per_request / rates[channel.name])
                    for sub_slice in time_slice.subdivide(sub_offset):
                        jobs[time_slice].append(
                            self._make_job(
                                datasource_rid, [channel], sub_slice, tags, buckets, resolution, timestamp_type
                            )
                        )

        return jobs

    def _rates_and_presence(
        self, numeric_channels: Sequence[Channel], time_range: _TimeRange, tags: Mapping[str, str] | None
    ) -> tuple[dict[str, float], dict[str, list[int]]]:
        """One bucketed-compute pass -> (per-channel rate, per-channel non-empty bucket timestamps).

        Channels with no data in range are absent from both maps.
        """
        start_ns, end_ns = time_range.start_time, time_range.end_time
        bucket_map = _channel_data_buckets(
            self._client, numeric_channels, start_ns, end_ns, tags, num_workers=self._num_workers
        )
        rates: dict[str, float] = {}
        presence: dict[str, list[int]] = {}
        for name, bkts in bucket_map.items():
            rate = _max_rate_from_buckets(bkts, start_ns, end_ns)
            if rate:
                rates[name] = rate
            non_empty = [b.timestamp for b in bkts if b.count > 0]
            if non_empty:
                presence[name] = non_empty

        empty = [c.name for c in numeric_channels if c.name not in rates]
        if empty:
            logger.info(
                "%d of %d numeric channels have no data in the requested range and will be skipped",
                len(empty),
                len(numeric_channels),
            )
        return rates, presence

    def _enum_presence(
        self, enum_channels: Sequence[Channel], time_range: _TimeRange, tags: Mapping[str, str] | None
    ) -> tuple[dict[str, list[int]], set[str]]:
        """Per enum (string) channel, non-empty bucket timestamps for pruning + undetermined channels.

        Returns ``(presence, undetermined)``: channels with determinable data map to their non-empty
        bucket timestamps (channels with no data are absent), and ``undetermined`` holds channels
        whose presence could not be computed (exported for all slices rather than dropped).
        """
        bucket_map, undetermined = _channel_enum_buckets(
            self._client, enum_channels, time_range.start_time, time_range.end_time, tags, num_workers=self._num_workers
        )
        presence: dict[str, list[int]] = {}
        for name, bkts in bucket_map.items():
            non_empty = [b.timestamp for b in bkts if b.frequencies]
            if non_empty:
                presence[name] = non_empty

        skipped = [c.name for c in enum_channels if c.name not in presence and c.name not in undetermined]
        if skipped:
            logger.info(
                "%d of %d enum channels have no data in the requested range and will be skipped",
                len(skipped),
                len(enum_channels),
            )
        return presence, undetermined

    def _make_job(
        self,
        datasource_rid: str,
        group: Sequence[Channel],
        time_slice: _TimeRange,
        tags: Mapping[str, str] | None,
        buckets: int | None,
        resolution: IntegralNanosecondsDuration | None,
        timestamp_type: _AnyExportableTimestampType,
    ) -> _ExportJob:
        return _ExportJob(
            datasource_rid=datasource_rid,
            channel_names=[c.name for c in group],
            channel_types={c.name: c.data_type for c in group},
            time_slice=time_slice,
            tags=dict(tags or {}),
            buckets=buckets,
            resolution=resolution,
            timestamp_type=timestamp_type,
            compression=self._compression,
        )

    @staticmethod
    def _slice_has_data(non_empty_bucket_timestamps: list[int] | None, time_slice: _TimeRange) -> bool:
        """True if any non-empty bucket falls within the time slice."""
        if not non_empty_bucket_timestamps:
            return False
        return any(time_slice.start_time <= ts < time_slice.end_time for ts in non_empty_bucket_timestamps)

    def _build_download_items(
        self,
        export_jobs: Mapping[_TimeRange, Sequence[_ExportJob]],
        output_dir: pathlib.Path,
        file_prefix: str,
    ) -> list[DownloadItem]:
        # Resolve each datasource once (cached) since many jobs share a datasource.
        datasource_rids = {job.datasource_rid for jobs in export_jobs.values() for job in jobs}
        datasources = {rid: self._client.get_datasource(rid) for rid in datasource_rids}

        items: list[DownloadItem] = []
        for slice_idx, time_slice in enumerate(sorted(export_jobs.keys())):
            for job_idx, job in enumerate(export_jobs[time_slice]):
                destination = output_dir / self._file_name(file_prefix, job, slice_idx, job_idx)
                provider = self._presigned_url_provider(job, datasources[job.datasource_rid])
                items.append(DownloadItem(provider=provider, destination=destination, part_size=self._part_size))
        return items

    @staticmethod
    def _file_name(file_prefix: str, job: _ExportJob, slice_idx: int, job_idx: int) -> str:
        datasource_short = job.datasource_rid.split(".")[-1]
        return f"{file_prefix}_{datasource_short}_s{slice_idx:04d}_g{job_idx:03d}.csv"

    def _presigned_url_provider(self, job: _ExportJob, datasource: DataSource) -> PresignedURLProvider:
        # Build the export request lazily on first fetch (it issues a get_channels call), then memoize
        # so re-signing on expiry doesn't rebuild it.
        cached_request: scout_dataexport_api.ExportDataRequest | None = None

        def fetch() -> str:
            nonlocal cached_request
            if cached_request is None:
                cached_request = job.export_request(datasource)
            response = self._generate_presigned_link(cached_request)
            url = response.presigned_url.url
            logger.debug(
                "Presigned export link ready (channels=%d, %.2f MB)",
                len(job.channel_names),
                response.file_size_bytes / 1e6,
            )
            # Large exports are written to S3 asynchronously; wait until the object is fully
            # materialized so the downloader never captures a partial (header-only) file.
            self._wait_until_materialized(url, response.file_size_bytes)
            return url

        return PresignedURLProvider(fetch_fn=fetch, ttl_secs=self._url_ttl_secs, skew_secs=self._url_skew_secs)

    def _generate_presigned_link(
        self, request: scout_dataexport_api.ExportDataRequest
    ) -> scout_dataexport_api.GeneratePresignedLinkResponse:
        """Generate a presigned export link, retrying transient (5xx / 429 / network) failures.

        Each link is a separate server-side export request, so a transient backend error on one file
        shouldn't fail the whole export; non-transient errors (e.g. 4xx) are raised immediately.
        """
        delay = 0.5
        for attempt in range(self._max_link_retries):
            try:
                return self._client._clients.dataexport.generate_export_channel_data_presigned_link(
                    self._client._clients.auth_header, request
                )
            except Exception as exc:
                if attempt == self._max_link_retries - 1 or not _is_transient_error(exc):
                    raise
                logger.warning(
                    "Presigned link generation failed (attempt %d/%d), retrying: %s",
                    attempt + 1,
                    self._max_link_retries,
                    exc,
                )
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
        raise AssertionError("unreachable")  # loop either returns or raises

    def _wait_until_materialized(self, url: str, expected_size: int) -> None:
        """Block until the object served at ``url`` reaches ``expected_size`` bytes (or timeout).

        The presigned export endpoint returns the authoritative final ``file_size_bytes`` before the
        S3 object is fully written; its served size grows until complete. Polling avoids downloading
        a partially-written file. Empty exports (tiny ``expected_size``) satisfy this immediately.
        """
        deadline = time.monotonic() + self._readiness_timeout
        delay = 0.5
        while True:
            served = self._served_size(url)
            if served is not None and served >= expected_size:
                return
            if time.monotonic() >= deadline:
                logger.warning(
                    "Export object not fully materialized after %.0fs (served=%s, expected=%d); proceeding anyway",
                    self._readiness_timeout,
                    served,
                    expected_size,
                )
                return
            time.sleep(delay)
            delay = min(delay * 2, 5.0)

    def _served_size(self, url: str) -> int | None:
        """Return the full object size S3 currently reports for ``url`` (via a ranged probe), or None."""
        try:
            resp = requests.get(url, headers={"Range": "bytes=0-0"}, timeout=self._timeout)
        except requests.RequestException:
            return None
        if resp.status_code not in (200, 206):
            return None
        content_range = resp.headers.get("Content-Range")
        if content_range:
            return int(content_range.split("/")[-1])
        content_length = resp.headers.get("Content-Length")
        return int(content_length) if content_length is not None else None

    @staticmethod
    def merge(
        paths: Sequence[str | pathlib.Path],
        *,
        timestamp_column: str = DEFAULT_EXPORTED_TIMESTAMP_COL_NAME,
    ) -> pl.LazyFrame:
        """Lazily reassemble exported CSV files into a unified, timestamp-sorted frame.

        Each file is a timestamp-keyed *column fragment*: within a time slice several files share the
        same timestamps but hold different channels, and (because channels are pruned/regrouped per
        slice) the same channel can appear in different column groupings across slices. So we stack
        every fragment (union of rows and columns, missing channels filled with null) and collapse
        rows that share a timestamp by taking the first non-null value per channel. This is robust to
        arbitrarily overlapping column sets (an outer-join-on-timestamp approach would mis-handle a
        channel that appears in two different groupings).

        The result is a ``pl.LazyFrame`` so the caller controls materialization: ``.collect()``,
        ``.collect(engine="streaming")`` for larger-than-RAM data, or ``.sink_parquet()`` /
        ``.sink_csv()`` to stream to disk.

        Args:
            paths: Paths to the CSV files to merge.
            timestamp_column: Name of the shared timestamp column to coalesce/sort on.

        Returns:
            A lazy frame producing the unified table.
        """
        resolved = [pathlib.Path(p) for p in paths]
        if not resolved:
            raise ValueError("No paths provided to merge")

        # Skip header-only files (no data rows). They contribute nothing, and because they have no
        # values polars infers their timestamp column as str -- which then fails to align against the
        # f64 timestamps of data-bearing files. (Header-only files can occur for channels whose
        # per-slice presence could not be determined and were exported defensively.) The check is
        # parse-free (no CSV type inference) so it can't trip over int-vs-float columns.
        non_empty = [path for path in resolved if PresignedExportHandler._has_data_rows(path)]
        if not non_empty:
            raise ValueError("No data rows found across the provided files")

        # Infer dtypes from the full file (infer_schema_length=None): exported numeric columns can look
        # integer for thousands of rows before a float appears, so a sampled inference mis-types them
        # as i64 and fails to parse the later float. Read only the header (infer_schema_length=0) for
        # the timestamp-column presence check.
        frames = []
        for path in non_empty:
            names = pl.scan_csv(path, infer_schema_length=0).collect_schema().names()
            if timestamp_column not in names:
                raise ValueError(f"File {path} has no '{timestamp_column}' column; cannot merge")
            frames.append(pl.scan_csv(path, infer_schema_length=None))

        # Union all fragments (diagonal_relaxed aligns columns by name and coerces mixed int/float
        # dtypes), then collapse rows sharing a timestamp. Each channel is populated in exactly one
        # fragment per timestamp, so first-non-null reconstructs the full row without conflicts.
        unified = pl.concat(frames, how="diagonal_relaxed")
        value_columns = [name for name in unified.collect_schema().names() if name != timestamp_column]
        merged = unified.group_by(timestamp_column).agg(pl.col(value_columns).drop_nulls().first())
        return merged.sort(timestamp_column)

    @staticmethod
    def _has_data_rows(path: pathlib.Path) -> bool:
        """Return True if the CSV has at least one non-empty data row (parse-free, early-exit)."""
        with path.open("r") as f:
            next(f, None)  # skip header
            return any(line.strip() for line in f)
