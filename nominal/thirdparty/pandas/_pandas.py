from __future__ import annotations

import logging
from datetime import datetime
from threading import Thread
from typing import Any, BinaryIO, Sequence, cast

from nominal_api.api import Timestamp

import pandas as pd
from nominal import ts
from nominal._utils import reader_writer
from nominal.core._utils import batched
from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.datasource import DataSource, _construct_export_request
from nominal.core.filetype import FileTypes

logger = logging.getLogger(__name__)


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
) -> Dataset:
    """Create a dataset in the Nominal platform from a pandas.DataFrame.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """
    # TODO(alkasm): use parquet instead of CSV as an intermediary

    def write_and_close(df: pd.DataFrame, w: BinaryIO) -> None:
        df.to_csv(w)
        w.close()

    with reader_writer() as (reader, writer):
        # write the dataframe to CSV in another thread
        t = Thread(target=write_and_close, args=(df, writer))
        t.start()
        dataset = client.create_dataset_from_io(
            reader,
            name,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=FileTypes.CSV,
            description=description,
            prefix_tree_delimiter=channel_name_delimiter,
        )
        t.join()
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


def channel_to_series(
    channel: Channel,
    start: datetime | ts.IntegralNanosecondsUTC | None = None,
    end: datetime | ts.IntegralNanosecondsUTC | None = None,
) -> pd.Series[Any]:
    """Retrieve the channel data as a pandas.Series.

    The index of the series is the timestamp of the data.
    The index name is "timestamp" and the series name is the channel name.

    Example:
    -------
    ```
    s = channel_to_series(channel)
    print(s.name, "mean:", s.mean())
    ```

    """
    start_time = ts._MIN_TIMESTAMP.to_api() if start is None else ts._SecondsNanos.from_flexible(start).to_api()
    end_time = ts._MAX_TIMESTAMP.to_api() if end is None else ts._SecondsNanos.from_flexible(end).to_api()
    body = channel._get_series_values_csv(start_time, end_time)
    df = pd.read_csv(body, parse_dates=["timestamp"], index_col="timestamp")
    return df[channel.name]


def channel_to_dataframe_decimated(
    channel: Channel,
    start: str | datetime | ts.IntegralNanosecondsUTC,
    end: str | datetime | ts.IntegralNanosecondsUTC,
    *,
    buckets: int | None = None,
    resolution: int | None = None,
) -> pd.DataFrame:
    """Retrieve the channel data as a pandas.DataFrame, decimated to the given buckets or resolution.

    Enter either the number of buckets or the resolution for the output.
    Resolution in picoseconds for picosecond-granularity dataset, nanoseconds otherwise.
    """
    if buckets is not None and resolution is not None:
        raise ValueError("Either buckets or resolution should be provided")

    result = channel._decimate_request(start, end, buckets, resolution)

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


def datasource_to_dataframe(
    datasource: DataSource,
    channel_exact_match: Sequence[str] = (),
    channel_fuzzy_search_text: str = "",
    start: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    end: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    tags: dict[str, str] | None = None,
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
        tags: Dictionary of tags to filter channels by
        start: The minimum data updated time to filter channels by
        end: The maximum data start time to filter channels by

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
    filtered_channels = datasource.search_channels(
        exact_match=channel_exact_match,
        fuzzy_search_text=channel_fuzzy_search_text,
    )

    batch_size = 20
    all_dataframes = []

    for channel_batch in batched(filtered_channels, batch_size):
        export_request = _construct_export_request(channel_batch, datasource.rid, start_time, end_time, tags)
        export_response = cast(
            BinaryIO,
            datasource._clients.dataexport.export_channel_data(datasource._clients.auth_header, export_request),
        )
        batch_df = pd.DataFrame(pd.read_csv(export_response))
        if not batch_df.empty:
            all_dataframes.append(batch_df)

    if not all_dataframes:
        logger.warning(f"No data found for export from datasource {datasource.rid}")
        raise RuntimeError(f"No data found for export from datasource {datasource.rid}")

    result_df = pd.concat(all_dataframes, axis=0)
    return result_df
