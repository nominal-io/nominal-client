from __future__ import annotations

import concurrent.futures
import logging
import pathlib
import tempfile
from datetime import datetime
from threading import Thread
from typing import Any, BinaryIO, Mapping, Sequence

import pandas as pd
from nominal_api import scout_dataexport_api
from nominal_api.api import Timestamp

from nominal import ts
from nominal._utils import batched, reader_writer
from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
)
from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile
from nominal.core.datasource import DataSource, _construct_export_request
from nominal.core.filetype import FileTypes
from nominal.core.run import Run

logger = logging.getLogger(__name__)


def upload_dataframe_to_dataset(
    dataset: Dataset,
    df: pd.DataFrame,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    *,
    wait_until_complete: bool = True,
    file_name: str | None = None,
    tag_columns: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
) -> DatasetFile:
    """Upload a pandas dataframe to an existing dataset as if it were a gzipped-CSV file

    Args:
        dataset: Dataset to upload the dataframe to
        df: Dataframe to upload to the dataset
        timestamp_column: Column containing timestamps to use for their respective rows
        timestamp_type: Type of timestamp, e.g., epoch_seconds, iso8601, etc.
        wait_until_complete: If true, block until data has been ingested
        file_name: Manually override the name of the filename given to the uploaded data.
            If not provided, defaults to using the dataset's name
        tag_columns: Mapping of column names => tag keys to use for their respective rows.
        tags: Mapping of key-value pairs to apply uniformly as tags to all data within the dataframe.
    """

    # TODO (drake): convert to parquet if/when parquet added as library dependency
    def write_and_close(df: pd.DataFrame, w: BinaryIO) -> None:
        df.to_csv(w, compression="gzip")
        w.close()

    with reader_writer() as (reader, writer):
        # Write the dataframe to .csv.gz and upload in background thread
        t = Thread(target=write_and_close, args=(df, writer))
        t.start()

        dataset_file = dataset.add_from_io(
            reader,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=FileTypes.CSV_GZ,
            file_name=file_name,
            tag_columns=tag_columns,
            tags=tags,
        )

        # Await data upload to complete
        t.join()

        if wait_until_complete:
            dataset_file.poll_until_ingestion_completed()

        return dataset_file


def upload_dataframe(
    client: NominalClient,
    df: pd.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    channel_name_delimiter: str | None = None,
    *,
    wait_until_complete: bool = True,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    tag_columns: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
) -> Dataset:
    """Create a dataset in the Nominal platform from a pandas.DataFrame.

    Args:
        client: Client instance to use for creating the dataset
        df: Dataframe to create a dataset from
        name: Name of the dataset to create, as well as filename for the uploaded "file".
        timestamp_column: Name of the column containing timestamp information for the dataframe
        timestamp_type: Type of the timestamp column, e.g. epoch_seconds, iso8601, etc.
        description: Description of the dataset to create
        channel_name_delimiter: Delimiter to use for folding channel view to a tree view.
        wait_until_complete: If true, wait until all data has been ingested successfully before returning
        labels: String labels to apply to the created dataset
        properties: String key-value pairs to apply to the created dataset
        tag_columns: Mapping of column name => tag key to apply to the respective rows of data
        tags: Mapping of key-value pairs to apply uniformly as tags to all data within the dataframe.

    Returns:
        Created dataset
    """
    dataset = client.create_dataset(
        name=name,
        description=description,
        labels=labels,
        properties=properties,
        prefix_tree_delimiter=channel_name_delimiter,
    )

    upload_dataframe_to_dataset(
        dataset,
        df,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
        wait_until_complete=wait_until_complete,
        file_name=name,
        tag_columns=tag_columns,
        tags=tags,
    )

    return dataset


def channel_to_series(
    channel: Channel,
    start: datetime | ts.IntegralNanosecondsUTC | None = None,
    end: datetime | ts.IntegralNanosecondsUTC | None = None,
    relative_to: datetime | ts.IntegralNanosecondsUTC | None = None,
    relative_resolution: ts._LiteralTimeUnit = "nanoseconds",
    *,
    enable_gzip: bool = True,
    tags: Mapping[str, str] | None = None,
) -> pd.Series[Any]:
    """Retrieve the channel data as a pandas.Series.

    The index of the series is the timestamp of the data.
    The index name is "timestamp" and the series name is the channel name.

    Use `relative_to` and `relative_resolution` to return timestamps relative to the given epoch.

    Example:
    -------
    ```
    s = channel_to_series(channel)
    print(s.name, "mean:", s.mean())
    ```

    """
    start_time = ts._MIN_TIMESTAMP.to_api() if start is None else ts._SecondsNanos.from_flexible(start).to_api()
    end_time = ts._MAX_TIMESTAMP.to_api() if end is None else ts._SecondsNanos.from_flexible(end).to_api()
    body = channel._get_series_values_csv(
        start_time,
        end_time,
        relative_to=relative_to,
        relative_resolution=relative_resolution,
        enable_gzip=enable_gzip,
        tags=tags,
    )
    df = pd.read_csv(
        body, parse_dates=["timestamp"], index_col="timestamp", compression="gzip" if enable_gzip else "infer"
    )
    return df[channel.name]


def channel_to_dataframe_decimated(
    channel: Channel,
    start: str | datetime | ts.IntegralNanosecondsUTC,
    end: str | datetime | ts.IntegralNanosecondsUTC,
    *,
    buckets: int | None = None,
    resolution: int | None = None,
    tags: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Retrieve the channel summary as a pandas.DataFrame, decimated to the given buckets or resolution.

    Enter either the number of buckets or the resolution for the output.
    Resolution in picoseconds for picosecond-granularity dataset, nanoseconds otherwise.
    """
    if buckets is not None and resolution is not None:
        raise ValueError("Either buckets or resolution should be provided")

    result = channel._decimate_request(start, end, tags=tags, buckets=buckets, resolution=resolution)

    # when there are less than 1000 points, the result is numeric
    # TODO(alkasm): why should this return differently shaped dataframes?
    if result.numeric is not None:
        df = pd.DataFrame(
            result.numeric.values,
            columns=["value"],
            index=[_to_pandas_timestamp(timestamp) for timestamp in result.numeric.timestamps],
        )
        df.index.name = "timestamp"
        return df

    if result.bucketed_numeric is None:
        raise ValueError("Unexpected response from compute service, bucketed_numeric should not be None")
    df = pd.DataFrame(
        [
            (bucket.min, bucket.max, bucket.mean, bucket.count, bucket.variance)
            for bucket in result.bucketed_numeric.buckets
        ],
        columns=["min", "max", "mean", "count", "variance"],
        index=[_to_pandas_timestamp(timestamp) for timestamp in result.bucketed_numeric.timestamps],
    )
    df.index.name = "timestamp"
    return df


def _to_pandas_timestamp(timestamp: Timestamp) -> pd.Timestamp:
    return pd.Timestamp(timestamp.seconds, unit="s", tz="UTC") + pd.Timedelta(timestamp.nanos, unit="ns")


def _to_pandas_unit(unit: ts._LiteralTimeUnit) -> str:
    return {
        "nanoseconds": "ns",
        "microseconds": "us",
        "milliseconds": "ms",
        "seconds": "s",
        "minutes": "m",
        "hours": "h",
    }[unit]


_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

# Tuning for presigned-URL caching during exports. TTL is generous because export URLs
# back large CSVs that may take a while to download; skew is a buffer to avoid handing
# out a URL whose deadline has already passed.
_EXPORT_URL_TTL_SECS = 600.0
_EXPORT_URL_SKEW_SECS = 15.0


def _get_renamed_timestamp_column(channels: list[Channel]) -> str:
    filtered_channel_names = set([ch.name for ch in channels])

    # Handle channel names that will be renamed during export
    renamed_timestamp_col = _EXPORTED_TIMESTAMP_COL_NAME
    if _EXPORTED_TIMESTAMP_COL_NAME in filtered_channel_names:
        idx = 1
        while True:
            other_col_name = f"timestamp.{idx}"
            if other_col_name not in filtered_channel_names:
                renamed_timestamp_col = other_col_name
                break
            else:
                idx += 1

    return renamed_timestamp_col


def _read_export_csv(
    path: pathlib.Path,
    channel_batch: Sequence[Channel],
    renamed_timestamp_col: str,
    enable_gzip: bool,
    relative_to: datetime | ts.IntegralNanosecondsUTC | None,
    datasource_rid: str,
) -> pd.DataFrame:
    """Read one batch's exported CSV from disk into a DataFrame indexed by timestamp.

    Returns an empty DataFrame with the batch's channel names as columns when the file
    contains no rows.
    """
    batch_df = pd.DataFrame(pd.read_csv(path, compression="gzip" if enable_gzip else "infer"))
    if batch_df.empty:
        channel_names = [ch.name for ch in channel_batch]
        logger.warning(
            "No data found for export for channels %s from datasource %s",
            channel_names,
            datasource_rid,
        )
        return pd.DataFrame({col: [] for col in channel_names + [_EXPORTED_TIMESTAMP_COL_NAME]}).set_index(
            _EXPORTED_TIMESTAMP_COL_NAME
        )

    if relative_to is None:
        batch_df[renamed_timestamp_col] = pd.to_datetime(batch_df[renamed_timestamp_col], format="ISO8601")
    return batch_df.set_index(renamed_timestamp_col)


def datasource_to_dataframe(  # noqa: PLR0912, PLR0915
    datasource: DataSource,
    channel_exact_match: Sequence[str] | None = None,
    channel_fuzzy_search_text: str | None = None,
    start: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    end: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    tags: Mapping[str, str] | None = None,
    enable_gzip: bool = True,
    *,
    channels: Sequence[Channel] | None = None,
    num_workers: int = 1,
    channel_batch_size: int = 20,
    relative_to: datetime | ts.IntegralNanosecondsUTC | None = None,
    relative_resolution: ts._LiteralTimeUnit = "nanoseconds",
) -> pd.DataFrame:
    """Download a dataset to a pandas dataframe, optionally filtering for only specific channels of the dataset.

    Args:
    ----
        datasource: The datasource to download data from
        channel_exact_match: Filter the returned channels to those whose names match all provided strings
            (case insensitive).
            For example, a channel named 'engine_turbine_rpm' would match against ['engine', 'turbine', 'rpm'],
            whereas a channel named 'engine_turbine_flowrate' would not!
        channel_fuzzy_search_text: Filters the returned channels to those whose names fuzzily match the provided
            string.
        channels: List of channels to fetch data for. If provided, supercedes search parameters of
            `channel_exact_match` and `channel_fuzzy_search_text`.
        tags: Dictionary of tags to filter channels by
        start: The minimum data updated time to filter channels by
        end: The maximum data start time to filter channels by
        enable_gzip: If true, use gzip when exporting data from Nominal. This will almost always make export
            faster and use less bandwidth.
        num_workers: Number of parallel threads used to request presigned export URLs from the backend
            (one API request per channel batch). The actual file downloads from S3 are run by
            `MultipartFileDownloader`, which uses its own cpu_count default for parallel ranged GETs and
            is not gated by this parameter. 4-8 is more than sufficient for the URL-fetch phase.
        channel_batch_size: Number of channels to request at a time per worker thread. Reducing this number may allow
            fetching a larger time duration (i.e., `end` - `start`), depending on how synchronized the timing is amongst
            the requested channels. This is a result of a limit of 10_000_000 unique timestamps returned per request,
            so reducing the number of channels will allow for a larger time window if channels come in at different
            times (e.g. channel A has timestamps 100, 200, 300... and channel B has timestamps 101, 201, 301, ...).
            This is particularly useful when combined with num_workers when attempting to maximally utilize a machine.
        relative_to: If provided, return timestamps relative to the given epoch time
        relative_resolution: If providing timestamps in relative time, the resolution to use

    Returns:
    -------
        A pandas dataframe whose index is the timestamp of the data, and column names match those of the selected
            channels.

    Example:
    -------
    ```
    rid = "..." # Taken from the UI or via the SDK
    dataset = client.get_dataset(rid)
    df = datasource_to_dataframe(dataset)
    print(df.head())  # Show first few rows of data
    ```

    """
    start_time = ts._SecondsNanos.from_flexible(start).to_api() if start else ts._MIN_TIMESTAMP.to_api()
    end_time = ts._SecondsNanos.from_flexible(end).to_api() if end else ts._MAX_TIMESTAMP.to_api()

    # Get all channels from the datasource
    if channels is None:
        channels = list(
            datasource.search_channels(
                exact_match=channel_exact_match or (),
                fuzzy_search_text=channel_fuzzy_search_text or "",
            )
        )
    elif channel_exact_match is not None or channel_fuzzy_search_text is not None:
        logger.warning(
            "'channel_exact_match' and 'channel_fuzzy_search_text' are ignored when a list of channels "
            "are provided to 'datasource_to_dataframe'."
        )

    if not channels:
        logger.warning("Requested data for no columns: returning empty dataframe")
        return pd.DataFrame({_EXPORTED_TIMESTAMP_COL_NAME: []}).set_index(_EXPORTED_TIMESTAMP_COL_NAME)

    # Warn about renamed channels
    renamed_timestamp_col = _get_renamed_timestamp_column(list(channels))

    channel_batches = list(batched(channels, channel_batch_size))
    batched_requests = [
        _construct_export_request(
            batch,
            start_time,
            end_time,
            tags=tags,
            enable_gzip=enable_gzip,
            timestamp_type=ts._to_export_timestamp_type(relative_to, relative_resolution),
        )
        for batch in channel_batches
    ]

    def _make_provider(request: scout_dataexport_api.ExportDataRequest) -> PresignedURLProvider:
        def fetch() -> str:
            response = datasource._clients.dataexport.generate_export_channel_data_presigned_link(
                datasource._clients.auth_header, request
            )
            return response.presigned_url.url

        return PresignedURLProvider(fetch_fn=fetch, ttl_secs=_EXPORT_URL_TTL_SECS, skew_secs=_EXPORT_URL_SKEW_SECS)

    providers = [_make_provider(req) for req in batched_requests]

    # Phase 1: warm up providers in parallel so the first URL fetch for each batch happens
    # concurrently (MultipartFileDownloader plans serially, so its own HEAD probe wouldn't).
    valid_batch_indices: list[int] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        warm_futures = {pool.submit(providers[i].get_url): i for i in range(len(providers))}
        for fut in concurrent.futures.as_completed(warm_futures):
            ex = fut.exception()
            if ex is not None:
                logger.error(
                    "Failed generating presigned link for channels %s from datasource %s",
                    [ch.name for ch in channel_batches[warm_futures[fut]]],
                    datasource.rid,
                    exc_info=ex,
                )
            else:
                valid_batch_indices.append(warm_futures[fut])

    # Phase 2: multipart-download every CSV in parallel, then read each from disk.
    suffix = ".csv.gz" if enable_gzip else ".csv"
    all_dataframes: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory(prefix="nominal-export-") as tmpdir:
        tmpdir_path = pathlib.Path(tmpdir)
        items: list[DownloadItem] = []
        dest_to_batch: dict[pathlib.Path, int] = {}
        for i in valid_batch_indices:
            dest = tmpdir_path / f"batch_{i}{suffix}"
            items.append(DownloadItem(provider=providers[i], destination=dest))
            dest_to_batch[dest] = i

        if items:
            # Don't gate the multipart downloader on `num_workers` (which is the
            # API-request fan-out for warm-up). Let it use its native cpu_count default
            # so ranged GETs across all files actually run in parallel.
            with MultipartFileDownloader.create() as downloader:
                results = downloader.download_files(items)

            for path, ex in results.failed.items():
                i = dest_to_batch[path]
                logger.error(
                    "Failed downloading data for channels %s from datasource %s",
                    [ch.name for ch in channel_batches[i]],
                    datasource.rid,
                    exc_info=ex,
                )

            for path in results.succeeded:
                all_dataframes.append(
                    _read_export_csv(
                        path,
                        channel_batches[dest_to_batch[path]],
                        renamed_timestamp_col,
                        enable_gzip,
                        relative_to,
                        datasource.rid,
                    )
                )
                # Free the CSV from disk as soon as it's parsed so the temp directory
                # holds at most one batch at a time, not the whole export.
                path.unlink(missing_ok=True)

    if not all_dataframes:
        logger.warning(f"No data found for export from datasource {datasource.rid}")
        all_column_names = [_EXPORTED_TIMESTAMP_COL_NAME] + [ch.name for ch in channels]
        return pd.DataFrame({col: [] for col in all_column_names}).set_index(_EXPORTED_TIMESTAMP_COL_NAME)

    try:
        result_df = pd.concat(all_dataframes, axis=1, join="outer", sort=True)
    except Exception as ex:
        raise RuntimeError(
            "Failed to join dataframe chunks-- ensure you have properly specified the tags for your datascope"
        ) from ex

    if renamed_timestamp_col is not None:
        result_df.index = result_df.index.rename(_EXPORTED_TIMESTAMP_COL_NAME)

    return result_df


def run_to_dataframe(
    run: Run,
    *,
    datascopes: Sequence[str] | None = None,
    channel_exact_match: Sequence[str] | None = None,
    num_workers: int = 1,
    channel_batch_size: int = 20,
) -> Mapping[str, pd.DataFrame]:
    """Download data from datasets in a run as pandas DataFrames keyed by datascope

    The run's `start` and `end` are used as the time bounds. Export is always gzip-compressed.

    Args:
    ----
        run: The run to download from. Must have at most one asset.
        datascopes: Optional list of datascopes from `run.list_datasets()`. If None
            (default), every datascope on the run is downloaded. Any datascope not
            present on the run causes the call to fail before any download begins.
        channel_exact_match: Substring AND match — every string must appear (case-insensitive)
            in a channel's name. Forwarded per-dataset to `datasource_to_dataframe`.
        num_workers: Parallel export workers per dataset.
        channel_batch_size: Channels per request per worker.

    Returns:
    -------
        Mapping of datascopes to pandas DataFrame.

    Raises:
    ------
        RuntimeError: if the run has more than one asset.
        ValueError: if any entry in `datascopes` is not present on the run.

    Example:
    -------
    ```
    run = client.get_run("...")  # rid taken from the UI or via the SDK

    # Download every datascope on the run
    dfs = run_to_dataframe(run)

    # Or filter to specific datascopes and channels
    dfs = run_to_dataframe(run, datascopes=["primary"], channel_exact_match=["engine", "rpm"])

    for datascope, df in dfs.items():
        print(datascope, df.shape)
    ```

    """
    if len(run.assets) > 1:
        raise RuntimeError(
            f"Run {run.rid!r} has {len(run.assets)} assets; run_to_dataframe only supports single-asset runs."
        )

    dataset_by_datascope = {scope: ds for scope, ds in run.list_datasets()}

    if datascopes is None:
        selected = dataset_by_datascope
    else:
        requested_datascopes = set(datascopes)
        selected = {name: dataset for name, dataset in dataset_by_datascope.items() if name in requested_datascopes}
        missing = sorted(requested_datascopes - selected.keys())
        if missing:
            raise ValueError(f"Run {run.rid!r} does not have datascope(s) {missing}")

    return {
        ref_name: datasource_to_dataframe(
            dataset,
            channel_exact_match=channel_exact_match,
            start=run.start,
            end=run.end,
            enable_gzip=True,
            num_workers=num_workers,
            channel_batch_size=channel_batch_size,
        )
        for ref_name, dataset in selected.items()
    }
