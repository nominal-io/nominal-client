import collections
import concurrent.futures
import datetime
import logging
from typing import Iterator, Mapping, Sequence

import polars as pl
from nominal._utils import LogTiming
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.client import NominalClient
from nominal.thirdparty.polars.export_handler import (
    DEFAULT_NUM_WORKERS,
    MAX_NUM_BUCKETS,
    ExportHandler,
    _ExportJob,
    _get_exported_timestamp_channel,
    _TimeRange,
)
from nominal.ts import (
    Epoch,
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    Iso8601,
    Relative,
    _AnyExportableTimestampType,
    _to_typed_timestamp_type,
)

# Number of points to export at once in a single request to the data export service.
# Nominal has a hard limit of 10 million unique timestaps within a single request,
# however, empirical performance is better with a smaller size
DEFAULT_POINTS_PER_REQUEST = 1_000_000

# Number of points to export within each dataframe exported at a time
DEFAULT_POINTS_PER_DATAFRAME = 25_000_000

# Maximum number of channels to get data for within a single request to Nominal
DEFAULT_CHANNELS_PER_REQUEST = 25

_INTERNAL_TS_COL = "__nmnl_ts__"  # internal join key, chosen to avoid collision with channel names

logger = logging.getLogger(__name__)


def _format_time_col(df: pl.DataFrame, time_col: str, job: _ExportJob) -> pl.DataFrame:
    typed_timestamp_type = _to_typed_timestamp_type(job.timestamp_type)

    if isinstance(typed_timestamp_type, (Relative, Epoch)):
        # Already numeric/relative per export service; no transform.
        return df.with_columns(pl.col(time_col).cast(pl.Float64).alias(time_col))
    elif isinstance(typed_timestamp_type, Iso8601):
        # Parse ISO8601 into timezone-aware datetime
        return df.with_columns(pl.col(time_col).str.strptime(pl.Datetime, strict=False, exact=False).alias(time_col))
    else:
        raise ValueError("Expected timestamp type to be a typed timestamp type")


def _export_job(job: _ExportJob, client: NominalClient) -> pl.DataFrame:
    if not job.channel_names:
        raise ValueError("No channels to extract")

    # Warn user about renamed channels, handle data with channel names of "timestamp"
    time_col = _get_exported_timestamp_channel(job.channel_names)

    datasource = client.get_datasource(job.datasource_rid)
    req = job.export_request(datasource)
    resp = client._clients.dataexport.export_channel_data(client._clients.auth_header, req)

    # force schema for export based on known channel types (helps if columns are all nan for a given part to prevent
    # that channel from loading as strings)
    schema: dict[str, pl.DataType] = {}
    for channel_name, data_type in job.channel_types.items():
        match data_type:
            case ChannelDataType.STRING:
                schema[channel_name] = pl.String()
            case ChannelDataType.DOUBLE:
                schema[channel_name] = pl.Float64()
            case ChannelDataType.INT:
                schema[channel_name] = pl.Int64()
            case _:
                logger.warning("Can't add missing channel %s to dataframe-- no known datatype!", channel_name)
                continue

    # Read CSV via Polars
    df = pl.read_csv(resp, schema_overrides=schema)
    if df.is_empty():
        logger.warning("No data found for export for channels %s", job.channel_names)
        return pl.DataFrame({col: [] for col in [*job.channel_names, time_col]})
    elif len(df[time_col].unique()) != len(df[time_col]):
        logger.error("Dataframe has duplicate timestamps! %s", df.head())

    # Convert string-based timestamps into native timestamp objects or floats based on desired export type
    df = _format_time_col(df, time_col, job)

    # Create internal timestamp column for consistent joins; keep original time_col out of the way
    df = df.rename({time_col: _INTERNAL_TS_COL})

    # Place timestamps first, then the data channels, with rows sorted by timestamps
    ordered_cols = [_INTERNAL_TS_COL] + [c for c in df.columns if c not in (_INTERNAL_TS_COL, time_col)]
    df = df.select(ordered_cols).sort(by=pl.col(_INTERNAL_TS_COL))

    # Add columns missing from the data to the dataframe for schema inference
    missing_channels = [channel_name for channel_name in job.channel_names if channel_name not in df.columns]
    if missing_channels:
        logger.warning("Found %d missing channels", len(missing_channels))
        channel_exprs = {}
        for channel_name in missing_channels:
            if channel_name in schema:
                channel_exprs[channel_name] = pl.lit(None).cast(schema[channel_name])
            else:
                logger.warning("Cannot infer type for channel %s, not exporting", channel_name)

        df = df.with_columns(**channel_exprs)

    return df


def _merge_dfs(dfs: Sequence[pl.DataFrame]) -> pl.DataFrame:
    if not dfs:
        return pl.DataFrame()

    # Vertically concat frames that have the exact same non-ts set of columns
    df_idx_by_channel_set: Mapping[frozenset[str], set[int]] = collections.defaultdict(set)
    for idx, df in enumerate(dfs):
        channel_cols = frozenset([c for c in df.columns if c != _INTERNAL_TS_COL])
        df_idx_by_channel_set[channel_cols].add(idx)

    logger.debug("Concatenating vertical columns")
    full_dfs: list[pl.LazyFrame] = []
    for channels, idxs in df_idx_by_channel_set.items():
        group_dfs = [dfs[i] for i in idxs if not dfs[i].is_empty()]
        if not group_dfs:
            logger.warning("Skipping empty dataframes for columns: %s", channels)
            continue

        sorted_dfs = sorted(group_dfs, key=lambda df: df[_INTERNAL_TS_COL].min() or 0)
        combined = pl.concat([df.lazy() for df in sorted_dfs], how="vertical", rechunk=True).sort(
            by=pl.col(_INTERNAL_TS_COL)
        )
        full_dfs.append(combined)

    if len(full_dfs) == 0:
        return pl.DataFrame()
    elif len(full_dfs) == 1:
        return full_dfs[0].sort(_INTERNAL_TS_COL).collect()

    logger.debug("Merging dataframes")

    # Outer-join all groups on internal ts
    merged = full_dfs[0]
    for next_df in full_dfs[1:]:
        merged = merged.join(next_df, on=_INTERNAL_TS_COL, how="full", coalesce=True)

    return merged.sort(_INTERNAL_TS_COL).collect()


class PolarsExportHandler(ExportHandler):
    """Manages streaming data out of Nominal using DataFrames.

    Steps:
    * If no bucket duration is provided, compute a max duration that fits the configured batch size.
    * Compute read schedule, channel groupings, and time slices.
    * For each time slice:
        * in parallel, fetch each channel group
        * stitch vertically within groups and then outer-join across groups on timestamp column
    * Yield merged DataFrame batches
    """

    def __init__(
        self,
        client: NominalClient,
        points_per_request: int = DEFAULT_POINTS_PER_REQUEST,
        points_per_dataframe: int = DEFAULT_POINTS_PER_DATAFRAME,
        channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST,
        num_workers: int = DEFAULT_NUM_WORKERS,
    ):
        """Initialize export handler"""
        super().__init__(
            client,
            points_per_request=points_per_request,
            points_per_dataframe=points_per_dataframe,
            channels_per_request=channels_per_request,
            num_workers=num_workers,
        )

    def export(
        self,
        channels: Sequence[Channel],
        start: IntegralNanosecondsUTC,
        end: IntegralNanosecondsUTC,
        tags: Mapping[str, str] | None = None,
        batch_duration: datetime.timedelta | None = None,
        timestamp_type: _AnyExportableTimestampType = "epoch_seconds",
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        join_batches: bool = True,
    ) -> Iterator[pl.DataFrame]:
        """Yield DataFrame slices"""
        # Ensure user has selected channels to export
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        # Ensure user has not selected incompatible decimation options
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        if resolution is not None:
            # If the batch duration is higher than this number, and data is actually downsampled with the
            # given resolution, then it would error today if the batch duration is any larger than this.
            computed_batch_duration = datetime.timedelta(seconds=(resolution * MAX_NUM_BUCKETS) / 1e9)
            if batch_duration is None:
                logger.info(
                    "Manually setting batch_duration to %fs (resolution=%dns)",
                    computed_batch_duration.total_seconds(),
                    resolution,
                )
                batch_duration = computed_batch_duration
            elif computed_batch_duration < batch_duration:
                logger.warning(
                    "Configured batch_duration of %fs would result in failing exports with resolution=%dns. "
                    "Setting batch_duration to %fs instead.",
                    batch_duration.total_seconds(),
                    resolution,
                    computed_batch_duration.total_seconds(),
                )
                batch_duration = computed_batch_duration

        # Determine download schedule
        export_jobs = self._compute_export_jobs(
            channels, _TimeRange(start, end), timestamp_type, tags or {}, buckets, resolution, batch_duration
        )
        time_column = _get_exported_timestamp_channel([ch.name for ch in channels])
        yield from self._export_dataframes(export_jobs, time_column, join_batches)

    def _export_dataframes(
        self, export_jobs: Mapping[_TimeRange, Sequence[_ExportJob]], time_column: str, join_batches: bool
    ) -> Iterator[pl.DataFrame]:
        # Kick off downloads
        with (
            LogTiming(f"Downloaded {len(export_jobs)} batches"),
            concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as pool,
        ):
            futures: list[concurrent.futures.Future[pl.DataFrame]] = []

            time_slices = sorted(export_jobs.keys())
            for idx, time_slice in enumerate(time_slices):
                logger.info(
                    "Starting to download data for slice %s (%d / %d)",
                    time_slice,
                    idx + 1,
                    len(time_slices),
                )
                with LogTiming(f"Downloaded data for slice {time_slice} ({idx + 1} / {len(time_slices)})"):
                    # Start by downloading if this is the first batch
                    if not futures:
                        futures = [
                            pool.submit(
                                _export_job,
                                task,
                                self._client,
                            )
                            for task in export_jobs[time_slice]
                        ]

                    results: list[pl.DataFrame] = []
                    for future_idx, future in enumerate(concurrent.futures.as_completed(futures)):
                        ex = future.exception()
                        if ex is not None:
                            logger.error("Failed to extract batch", exc_info=ex)
                            continue

                        res = future.result()
                        logger.info("Finished extracting batch %d/%d", future_idx + 1, len(futures))
                        if join_batches:
                            results.append(res)
                        elif res.is_empty():
                            continue
                        else:
                            yield res.rename({_INTERNAL_TS_COL: time_column})

                    # Schedule next batch of downloads before starting merge
                    if idx < len(time_slices) - 1:
                        futures = [
                            pool.submit(_export_job, task, self._client) for task in export_jobs[time_slices[idx + 1]]
                        ]

                if join_batches:
                    with LogTiming(f"Merged {len(results)} exports"):
                        logger.info("Merging dataframes")
                        merged_df = _merge_dfs(results)
                        if merged_df.is_empty():
                            logger.warning("Dataframe empty after merging...")
                        else:
                            yield merged_df.rename({_INTERNAL_TS_COL: time_column})
