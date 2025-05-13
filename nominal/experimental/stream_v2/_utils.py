from datetime import datetime
from typing import Dict, List as TypeList, Mapping, Optional, Tuple, TypeAlias

import numpy as np
import pandas as pd
from polars import List

from nominal.core._queueing import TimeArray, ValueArray, TagsArray

# Type alias for channel data tuple
ChannelData: TypeAlias = Tuple[TimeArray, TimeArray, ValueArray, int]  # (seconds, nanoseconds, values, count)
ChannelDict: TypeAlias = Dict[str, ChannelData]  # Dictionary mapping channel names to their data

# Type alias for batch item used in write_stream
BatchChunkItem: TypeAlias = Tuple[str, TimeArray, TimeArray, ValueArray, TagsArray]  # (channel_name, seconds, nanos, values, tags)
BatchChunk: TypeAlias = TypeList[BatchChunkItem]

def prepare_df_for_upload(
    df: pd.DataFrame, timestamp_column: str
) -> Tuple[Optional[ChannelDict], int]:
    
    """Pre-process DataFrame to prepare timestamps and convert to numeric values"""
    if timestamp_column not in df.columns or df[timestamp_column].empty:
        return None, 0

    # Convert timestamps to seconds and nanos once
    ts_epoch_stamps = df[timestamp_column].map(datetime.timestamp)
    ts_seconds = ts_epoch_stamps.astype(np.int64)
    ts_nanos = ((ts_epoch_stamps - ts_seconds) * 1e9).astype(np.int64)

    discarded_total = 0
    channel_data = {}

    for column in df.columns:
        if column == timestamp_column:
            continue

        # Convert to numeric, coercing errors to NaN and filter
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        valid_mask = ~numeric_series.isna()
        valid_indices = valid_mask.index[valid_mask]

        discarded = len(numeric_series) - len(valid_indices)
        discarded_total += discarded

        if len(valid_indices) == 0:
            continue

        # Get the data only for valid indices
        channel_values = numeric_series.loc[valid_indices].astype(np.float64)
        channel_seconds = ts_seconds.loc[valid_indices]
        channel_nanos = ts_nanos.loc[valid_indices]

        # Convert pandas Series to numpy arrays before storing
        channel_data[column] = (
            channel_seconds.to_numpy(),
            channel_nanos.to_numpy(),
            channel_values.to_numpy(),
            len(valid_indices)
        )

    return channel_data, discarded_total


def split_into_chunks(
    channel_data: ChannelDict, target_size: int = 50000, tags: Optional[Mapping[str, str]] = None
) -> TypeList[BatchChunk]:
    """Split channel data into chunks of approximately target_size points.
    
    Returns:
        A list of BatchChunks, where each BatchChunk is a list of BatchItems.
        Each BatchItem is a tuple of (channel_name, seconds, nanos, values, tags).
    """
    chunks: TypeList[BatchChunk] = []
    current_chunk: BatchChunk = []
    current_size = 0
    
    tags_array = tags or {}  # Default to empty dict if no tags provided

    for channel, (seconds, nanos, values, count) in channel_data.items():
        if current_size + count > target_size and current_size > 0:
            # Start a new chunk if adding this channel would exceed target size
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0

        # Create a BatchItem with the channel name
        batch_item = (channel, seconds, nanos, values, tags_array)
        current_chunk.append(batch_item)
        current_size += count

    # Add the last chunk if not empty
    if current_chunk:
        chunks.append(current_chunk)

    return chunks
