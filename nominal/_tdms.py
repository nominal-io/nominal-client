from __future__ import annotations

import functools
import logging
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from nptdms import TdmsChannel, TdmsFile, TdmsGroup

logger = logging.getLogger(__name__)


def tdms_with_time_column_to_pandas(path: Path, timestamp_column: str) -> pd.DataFrame:
    group_dfs: list[pd.DataFrame] = []

    with TdmsFile.open(path) as tdms_file:
        for group, time_channel in _get_groups_with_time_channel(tdms_file.groups(), timestamp_column):
            channels_to_export: dict[str, pd.Series[Any]] = {}
            for channel in _get_export_channels(group.channels(), time_channel, timestamp_column):
                channel_name = _create_channel_name(group, channel)
                channels_to_export[channel_name] = pd.Series(data=channel.read_data(), index=time_channel.read_data())

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


def tdms_with_waveform_props_to_pandas(path: Path, timestamp_column: str) -> pd.DataFrame:
    channels_to_export: dict[str, pd.Series[Any]] = {}
    with TdmsFile.open(path) as tdms_file:
        group: TdmsGroup
        for group in tdms_file.groups():
            for channel in _filter_waveform_channels(group.channels()):
                channel_name = _create_channel_name(group, channel)
                channels_to_export[channel_name] = pd.Series(
                    data=channel.read_data(), index=channel.time_track(absolute_time=True, accuracy="ns")
                )

    df = pd.DataFrame.from_dict(channels_to_export)

    # format for nominal upload
    df.index = df.index.set_names(timestamp_column, level=None)
    df = df.reset_index()
    df[timestamp_column] = df[timestamp_column].astype(np.int64)

    return df


def _filter_waveform_channels(channels: Iterable[TdmsChannel]) -> Iterable[TdmsChannel]:
    """Skip channels that do not have the required waveform properties to construct a time track"""
    for channel in channels:
        if "wf_increment" in channel.properties and "wf_start_time" in channel.properties:
            yield channel
        else:
            logger.warning(
                f"skipping channel {channel.name!r} because it does not have waveform properties "
                "'wf_increment' and 'wf_start_time'"
            )


def _create_channel_name(group: TdmsGroup, channel: TdmsChannel) -> str:
    return f"{group.name.replace(' ', '_')}.{channel.name.replace(' ', '_')}"


def _get_groups_with_time_channel(
    groups: Iterable[TdmsGroup], timestamp_column: str
) -> Iterable[tuple[TdmsGroup, TdmsChannel]]:
    """Yield groups that contain a channel with the specified timestamp_column."""
    for group in groups:
        found = False
        for channel in group.channels():
            if channel.name == timestamp_column:
                yield group, channel
                found = True
                break
        if not found:
            logger.warning(
                f"skipping channel group {channel.group_name!r} because expected timestamp_column "
                f"{timestamp_column!r} does not exist",
            )


def _get_export_channels(
    channels: Iterable[TdmsChannel], time_channel: TdmsChannel, timestamp_column: str
) -> Iterable[TdmsChannel]:
    """Skip the timestamp channel and any channel that does not have the same length as the timestamp channel."""
    for channel in channels:
        if len(channel) != len(time_channel):
            logger.warning(
                f"skipping channel {channel.name!r} because length does not match {timestamp_column!r}",
            )
        elif channel.name != timestamp_column:
            yield channel
