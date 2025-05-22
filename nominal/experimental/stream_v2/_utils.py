from datetime import datetime
from typing import Dict, Mapping, Optional, Tuple, TypeAlias
from typing import List as TypeList

import numpy as np
import pandas as pd

from nominal.core._queueing import BatchV2, TagsArray, TimeArray, ValueArray

# Type alias for channel data tuple
ChannelData: TypeAlias = Tuple[TimeArray, TimeArray, ValueArray, int]  # (seconds, nanoseconds, values, count)
ChannelDict: TypeAlias = Dict[str, ChannelData]  # Dictionary mapping channel names to their data

# Type alias for batch item used in write_stream
BatchChunkItem: TypeAlias = Tuple[
    str, TimeArray, TimeArray, ValueArray, TagsArray
]  # (channel_name, seconds, nanos, values, tags)
BatchChunk: TypeAlias = TypeList[BatchChunkItem]


def prepare_df_for_upload(df: pd.DataFrame, timestamp_column: str) -> Optional[ChannelDict]:
    """Pre-process DataFrame to prepare timestamps and convert to numeric values"""
    if timestamp_column not in df.columns or df[timestamp_column].empty:
        return None

    # Convert timestamps to seconds and nanos once
    ts_epoch_stamps = df[timestamp_column].map(datetime.timestamp)
    ts_seconds = ts_epoch_stamps.astype(np.int64)
    ts_nanos = ((ts_epoch_stamps - ts_seconds) * 1e9).astype(np.int64)

    channel_data = {}

    for column in df.columns:
        if column == timestamp_column:
            continue

        # Convert to numeric, coercing errors to NaN and filter
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        valid_mask = ~numeric_series.isna()
        valid_indices = valid_mask.index[valid_mask]

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
            len(valid_indices),
        )

    return channel_data


def split_into_chunks(
    channel_data: ChannelDict, target_size: int = 50000, tags: Optional[Mapping[str, str]] = None
) -> TypeList[TypeList[BatchV2]]:
    """Split channel data into chunks of approximately target_size points.

    Each channel's data is kept intact (not split across chunks). If adding a channel
    would exceed the target_size for the current chunk, a new chunk is started.

    Args:
        channel_data: Dictionary mapping channel names to their data
        target_size: Maximum target size for each chunk
        tags: Optional mapping of tags to include with each batch item

    Returns:
        A list of chunks, where each chunk is a list of BatchV2 objects.
    """
    tags_dict = tags or {}
    chunks: TypeList[TypeList[BatchV2]] = []
    current_chunk: TypeList[BatchV2] = []
    points_in_chunk = 0

    for channel_name, (seconds, nanoseconds, values, point_count) in channel_data.items():
        # Start a new chunk if adding this channel would exceed target size
        if points_in_chunk + point_count > target_size and points_in_chunk > 0:
            chunks.append(current_chunk)
            current_chunk = []
            points_in_chunk = 0

        # Create BatchV2 object directly instead of tuple
        batch = BatchV2(
            channel_name=channel_name,
            seconds=seconds,
            nanos=nanoseconds,
            values=values,
            tags=tags_dict,
        )

        current_chunk.append(batch)
        points_in_chunk += point_count

    # Add the final chunk if not empty
    if current_chunk:
        chunks.append(current_chunk)

    return chunks
