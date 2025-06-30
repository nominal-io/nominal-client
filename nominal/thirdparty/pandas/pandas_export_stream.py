from __future__ import annotations

import collections
import logging
from typing import BinaryIO, Sequence

import pandas as pd
from babel.dates import format_datetime

from nominal.core.export_stream import ExportStream
from nominal.core.read_stream_base import ExportJob
from nominal.ts import (
    Custom,
    Epoch,
    Iso8601,
    Relative,
    _LiteralTimeUnit,
    _to_typed_timestamp_type,
)

logger = logging.getLogger(__name__)


def _to_pandas_unit(unit: _LiteralTimeUnit) -> str:
    return {
        "nanoseconds": "ns",
        "microseconds": "us",
        "milliseconds": "ms",
        "seconds": "s",
        "minutes": "m",
        "hours": "h",
    }[unit]


_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"


def _get_renamed_timestamp_column(channel_names: list[str]) -> str:
    # Handle channel names that will be renamed during export
    renamed_timestamp_col = _EXPORTED_TIMESTAMP_COL_NAME
    if _EXPORTED_TIMESTAMP_COL_NAME in channel_names:
        idx = 1
        while True:
            other_col_name = f"timestamp.{idx}"
            if other_col_name not in channel_names:
                renamed_timestamp_col = other_col_name
                break
            else:
                idx += 1

    return renamed_timestamp_col


class PandasExportStream(ExportStream[pd.DataFrame]):
    @classmethod
    def _stream_export(cls, stream: BinaryIO, task: ExportJob) -> pd.DataFrame:
        channel_names = [ch.name for ch in task.channels]  # list(task.value.channel_sources.keys())

        # Warn about renamed channels
        renamed_timestamp_col = _get_renamed_timestamp_column(channel_names)

        # Read data into dataframe
        batch_df = pd.DataFrame(pd.read_csv(stream, compression="gzip"))
        if batch_df.empty:
            logger.warning(
                "No data found for export for channels %s",
                channel_names,
            )
            return pd.DataFrame({col: [] for col in channel_names + [_EXPORTED_TIMESTAMP_COL_NAME]}).set_index(
                _EXPORTED_TIMESTAMP_COL_NAME
            )

        typed_timestamp_type = _to_typed_timestamp_type(task.timestamp_type)
        time_col = pd.to_datetime(batch_df[renamed_timestamp_col], format="ISO8601", utc=True)

        if isinstance(typed_timestamp_type, Relative):
            pd_unit = _to_pandas_unit(typed_timestamp_type.unit)
            offset_time_col = time_col - pd.to_datetime(typed_timestamp_type.start, utc=True)
            batch_df[renamed_timestamp_col] = offset_time_col / pd.Timedelta(f"1{pd_unit}")
        elif isinstance(typed_timestamp_type, Epoch):
            pd_unit = _to_pandas_unit(typed_timestamp_type.unit)
            offset_time_col = time_col - pd.to_datetime(0, utc=True)
            batch_df[renamed_timestamp_col] = offset_time_col / pd.Timedelta(f"1{pd_unit}")
        elif isinstance(typed_timestamp_type, Custom):
            batch_df[renamed_timestamp_col] = pd.Series(
                [
                    format_datetime(stamp, typed_timestamp_type.format, tzinfo=stamp.tz, locale="en_US")
                    for stamp in time_col
                ]
            )
        elif isinstance(typed_timestamp_type, Iso8601):
            # do nothing-- already in iso format
            batch_df[renamed_timestamp_col] = time_col
        else:
            raise ValueError("Expected timestamp type to be a typed timestamp type")

        return batch_df.set_index(renamed_timestamp_col)

    @classmethod
    def _merge_exports(cls, exports: Sequence[pd.DataFrame]) -> pd.DataFrame:
        if not exports:
            return pd.DataFrame()

        # First, vertically concatenate exports that have the same set of columns
        channel_set_occurrences: collections.defaultdict[tuple[str, ...], int] = collections.defaultdict(int)
        for export in exports:
            channel_set_occurrences[tuple(export.columns)] += 1

        merged_exports = [export for export in exports if channel_set_occurrences[tuple(export.columns)] == 1]
        for columns, occurrences in channel_set_occurrences.items():
            if occurrences == 1:
                continue

            idxs = [idx for idx, df in enumerate(exports) if all([col in df.columns for col in columns])]
            merged_exports.append(pd.concat([exports[idx] for idx in idxs], sort=True))

        # Next, horizontally concatenate exports
        if len(merged_exports) == 1:
            return merged_exports[0]
        else:
            return merged_exports[0].join(list(merged_exports[1:]), how="outer", sort=True)
