from __future__ import annotations

import functools
import logging
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, Tuple

import numpy as np
import numpy.typing as npt
import pandas as pd

from nominal import ts
from nominal.core._types import PathLike
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile
from nominal.tdms._reader import Channel, Group, NiTimestamp, TdmsReader
from nominal.tdms._time import NS_PER_SECOND, is_timestamp_dtype, timestamp_to_ns, timestamps_to_ns, waveform_ns
from nominal.thirdparty.pandas import upload_dataframe, upload_dataframe_to_dataset

logger = logging.getLogger(__name__)


def _tdms_to_dataframe(
    file: PathLike,
    timestamp_column: str | None = None,
    timestamp_type: ts._AnyTimestampType | None = None,
) -> Tuple[str, ts._AnyTimestampType, pd.DataFrame]:
    """Returns tuple of timestamp column, timestamp type, and dataframe"""
    tdms_path = Path(file)

    use_waveform = timestamp_column is None and timestamp_type is None
    if use_waveform:
        timestamp_column = "__nominal_ts__"
        timestamp_type = ts.EPOCH_NANOSECONDS
    elif timestamp_column is None or timestamp_type is None:
        raise ValueError(
            f"Cannot upload tdms {tdms_path}-- either both, or neither, of "
            "`timestamp_column` and `timestamp_type` must be provided"
        )

    if use_waveform:
        df = _tdms_with_waveform_props_to_pandas(tdms_path, timestamp_column)
    else:
        df = _tdms_with_time_column_to_pandas(tdms_path, timestamp_column)

    return timestamp_column, timestamp_type, df


def upload_tdms_to_dataset(
    dataset: Dataset,
    file: PathLike,
    timestamp_column: str | None = None,
    timestamp_type: ts._AnyTimestampType | None = None,
    *,
    wait_until_complete: bool = True,
    file_name: str | None = None,
    tag_columns: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
) -> DatasetFile:
    """Process and upload a tdms file to an existing dataset as if it were a gzipped-CSV file

    Args:
        dataset: Dataset to upload the dataframe to
        file: Path to the TDMS file to parse and upload
        timestamp_column: Column containing timestamps to use for their respective rows
            NOTE: if provided, only groups containing a signal of this name will be uploaded.
                  Furthermore, the length of all data columns must match their respective timestamp columns.
            NOTE: if not provided, TDMS channel properties must have both a `wf_increment` and `wf_start_time`
                  property to be uploaded.
        timestamp_type: Type of timestamp, e.g., epoch_seconds, iso8601, etc.
        wait_until_complete: If true, block until data has been ingested
        file_name: Manually override the name of the filename given to the uploaded data.
            If not provided, defaults to using the dataset's name
        tag_columns: Mapping of column names => tag keys to use for their respective rows.
        tags: Mapping of key-value pairs to apply uniformly as tags to all data within the dataframe.

    Channels will be named as f"{group_name}.{channel_name}", with spaces replaced with underscores.

    NOTE: `timestamp_column` and `timestamp_type` must both be provided or excluded together.
    """
    timestamp_column, timestamp_type, df = _tdms_to_dataframe(file, timestamp_column, timestamp_type)
    return upload_dataframe_to_dataset(
        dataset,
        df,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
        wait_until_complete=wait_until_complete,
        file_name=file_name,
        tag_columns=tag_columns,
        tags=tags,
    )


def upload_tdms(
    client: NominalClient,
    file: PathLike,
    name: str | None = None,
    description: str | None = None,
    timestamp_column: str | None = None,
    timestamp_type: ts._AnyTimestampType | None = None,
    *,
    wait_until_complete: bool = True,
    channel_name_delimiter: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    tag_columns: Mapping[str, str] | None = None,
    tags: Mapping[str, str] | None = None,
) -> Dataset:
    """Create a dataset in the Nominal platform from a tdms file.

    See `upload_tdms_to_dataset` for a description of arguments available, or
    `nominal.thirdparty.pandas.upload_dataframe` for a description of dataset creation arguments available.
    """
    if name is None:
        name = Path(file).name

    timestamp_column, timestamp_type, df = _tdms_to_dataframe(file, timestamp_column, timestamp_type)
    return upload_dataframe(
        client,
        df,
        name=name,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
        description=description,
        wait_until_complete=wait_until_complete,
        channel_name_delimiter=channel_name_delimiter,
        labels=labels,
        properties=properties,
        tag_columns=tag_columns,
        tags=tags,
    )


def _channel_values(channel: Channel) -> npt.NDArray[Any]:
    """A channel's samples as a pandas-ready array.

    TDMS timestamp channels are read as raw seconds-and-fractions structs and
    become datetime64[ns]; everything else -- numbers, booleans, strings -- is
    returned as read.
    """
    values = channel.values(0, len(channel))
    if is_timestamp_dtype(values.dtype):
        return timestamps_to_ns(values).astype("datetime64[ns]")
    return values


def _waveform_index(channel: Channel) -> npt.NDArray[Any]:
    """Epoch-nanosecond timestamps implied by a channel's waveform properties.

    Sample i is stamped ``wf_start_time + wf_start_offset + i * wf_increment``,
    in integer arithmetic throughout so the result is exact to the nanosecond.
    """
    properties = channel.properties
    start = properties["wf_start_time"]
    assert isinstance(start, NiTimestamp)  # guaranteed by _filter_waveform_channels
    start_ns = timestamp_to_ns(start) + round(_number(properties.get("wf_start_offset", 0.0)) * NS_PER_SECOND)
    return waveform_ns(start_ns, 0, len(channel), _number(properties["wf_increment"]) * NS_PER_SECOND)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, np.integer, np.floating)):
        raise TypeError(f"expected a number, got {type(value).__name__}")
    return float(value)


def _tdms_with_time_column_to_pandas(path: Path, timestamp_column: str) -> pd.DataFrame:
    group_dfs: list[pd.DataFrame] = []

    with TdmsReader.open(path) as tdms_file:
        for group, time_channel in _get_groups_with_time_channel(tdms_file.groups(), timestamp_column):
            time_values = _channel_values(time_channel)
            channels_to_export: dict[str, pd.Series[Any]] = {}
            for channel in _get_export_channels(group.channels(), time_channel, timestamp_column):
                channel_name = _create_channel_name(group, channel)
                channels_to_export[channel_name] = pd.Series(data=_channel_values(channel), index=time_values)

            group_df = pd.DataFrame.from_dict(channels_to_export)
            group_dfs.append(group_df)

    # format for nominal upload
    df = functools.reduce(
        lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how="outer"),
        group_dfs,
    )
    df.index = df.index.set_names(timestamp_column, level=None)
    df = df.reset_index()

    return df


def _tdms_with_waveform_props_to_pandas(path: Path, timestamp_column: str) -> pd.DataFrame:
    channels_to_export: dict[str, pd.Series[Any]] = {}
    with TdmsReader.open(path) as tdms_file:
        group: Group
        for group in tdms_file.groups():
            for channel in _filter_waveform_channels(group.channels()):
                channel_name = _create_channel_name(group, channel)
                channels_to_export[channel_name] = pd.Series(
                    data=_channel_values(channel), index=_waveform_index(channel)
                )

    df = pd.DataFrame.from_dict(channels_to_export)

    # format for nominal upload
    df.index = df.index.set_names(timestamp_column, level=None)
    df = df.reset_index()
    df[timestamp_column] = df[timestamp_column].astype(np.int64)

    return df


def _filter_waveform_channels(channels: Iterable[Channel]) -> Iterable[Channel]:
    """Skip channels whose waveform properties cannot place their samples in time"""
    for channel in channels:
        if not _readable(channel):
            continue
        properties = channel.properties
        if "wf_increment" not in properties or "wf_start_time" not in properties:
            logger.warning(
                f"skipping channel {channel.name!r} because it does not have waveform properties "
                "'wf_increment' and 'wf_start_time'"
            )
        elif not isinstance(properties["wf_start_time"], NiTimestamp):
            logger.warning(f"skipping channel {channel.name!r} because 'wf_start_time' is not a timestamp")
        else:
            yield channel


def _readable(channel: Channel) -> bool:
    """Whether the channel's samples can become a DataFrame column, logging why not.

    Complex and void channels have no representation in Nominal, and a channel
    the reader cannot produce (an unknown scale stage, an unknown DAQmx raw
    type) says so in ``unsupported``.
    """
    if channel.unsupported is not None:
        logger.warning(f"skipping channel {channel.name!r}: {channel.unsupported}")
        return False
    if channel.dtype is None or channel.dtype.kind == "c":
        logger.warning(f"skipping channel {channel.name!r}: {channel.dtype or 'void'} data has no representation")
        return False
    return True


def _create_channel_name(group: Group, channel: Channel) -> str:
    return f"{group.name.replace(' ', '_')}.{channel.name.replace(' ', '_')}"


def _get_groups_with_time_channel(groups: Iterable[Group], timestamp_column: str) -> Iterable[tuple[Group, Channel]]:
    """Yield groups that contain a channel with the specified timestamp_column."""
    for group in groups:
        time_channel = next((channel for channel in group.channels() if channel.name == timestamp_column), None)
        if time_channel is None:
            logger.warning(
                f"skipping channel group {group.name!r} because expected timestamp_column "
                f"{timestamp_column!r} does not exist",
            )
        elif time_channel.unsupported is not None:
            logger.warning(f"skipping channel group {group.name!r}: {timestamp_column!r} {time_channel.unsupported}")
        else:
            yield group, time_channel


def _get_export_channels(
    channels: Iterable[Channel], time_channel: Channel, timestamp_column: str
) -> Iterable[Channel]:
    """Skip the timestamp channel and any channel that cannot be read or does not match its length."""
    for channel in channels:
        if channel.name == timestamp_column or not _readable(channel):
            continue
        if len(channel) != len(time_channel):
            logger.warning(
                f"skipping channel {channel.name!r} because length does not match {timestamp_column!r}",
            )
        else:
            yield channel
