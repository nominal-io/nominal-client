from __future__ import annotations

import abc
import collections
import logging
import multiprocessing
import time
from typing import Generic, Iterator, Mapping, Sequence, TypeVar

import pandas as pd
from pandas._typing import DtypeObj

from nominal._utils.threading_tools import StoppableQueue
from nominal.core.channel import Channel, ChannelDataType
from nominal.ts import IntegralNanosecondsUTC, _LiteralTimeUnit

logger = logging.getLogger(__name__)


def to_api_json_timestamp(timestamp: IntegralNanosecondsUTC) -> dict[str, int]:
    return {
        "seconds": int(timestamp / 1e9),
        "nanos": int(timestamp % 1e9),
    }


def to_api_dtype(dtype: DtypeObj) -> str:
    # The linter prefers using `is`, `is not`, or `isinstance` for the following checks,
    # but they don't actually work without much more specific types being used unless
    # `==` is used for the following checks
    if dtype == object:  # noqa: E721
        return "strings"
    elif dtype == int:  # noqa: E721
        return "ints"
    elif dtype == float:  # noqa: E721
        return "doubles"
    else:
        raise ValueError(f"Unknown datatype for streaming data: {dtype}")


def to_pandas_unit(unit: _LiteralTimeUnit) -> str:
    return {
        "nanoseconds": "ns",
        "microseconds": "us",
        "milliseconds": "ms",
        "seconds": "s",
        "minutes": "m",
        "hours": "h",
    }[unit]


def extract_batches_from_dataframe(
    df: pd.DataFrame, timestamp_column: str, max_batch_size: int
) -> Iterator[tuple[str, pd.DataFrame]]:
    if timestamp_column not in df.columns:
        raise ValueError(f"Timestamp column '{timestamp_column}' not found in DataFrame.")
    elif len(df.columns) != len(set(df.columns)):
        raise ValueError(f"Dataframe has duplicate columns: {df.columns}")

    valid_df = df[df[timestamp_column].notna()]
    for col_name in valid_df.columns:
        if col_name == timestamp_column:
            continue

        # 1. Select the current data column and the timestamp column
        # 2. Filter out rows where the current data column is null
        #    This ensures we only process pairs where both value and timestamp are valid
        filtered_df = valid_df[valid_df[col_name].notna()][[col_name, timestamp_column]]

        # If no non-null pairs exist for this column, skip it
        if filtered_df.empty:
            continue

        # Iterate through the column in batches using slicing
        num_rows_filtered = filtered_df.shape[0]
        for offset in range(0, num_rows_filtered, max_batch_size):
            df_slice = filtered_df.iloc[offset : min(offset + max_batch_size, num_rows_filtered)]
            if df_slice.empty:
                continue

            yield col_name, df_slice


def group_channels_by_datatype(channels: Sequence[Channel]) -> Mapping[ChannelDataType, Sequence[Channel]]:
    """Partition the provided channels by data type.

    Channels with no datatype are grouped into the UNKNOWN partition of channels.

    Args:
        channels: Channels to partition
    Returns:
        Mapping of data type to a list of the corresponding channels
    """
    channel_groups = collections.defaultdict(list)
    for channel in channels:
        if channel.data_type:
            channel_groups[channel.data_type].append(channel)
        else:
            channel_groups[ChannelDataType.UNKNOWN].append(channel)
    return {**channel_groups}


InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Worker(abc.ABC, Generic[InputT]):
    def __init__(
        self,
        *,
        input_queue: StoppableQueue[InputT],
        logger: logging.Logger | None = None,
        exit_on_exception: bool = False,
    ):
        self._input_queue = input_queue
        self._logger = logger
        self._exit_on_exception = exit_on_exception

    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def process(self, task_input: InputT) -> bool: ...

    @property
    def logger(self) -> logging.Logger:
        """Logger instance to use for logging"""
        if self._logger is None:
            self._logger = logging.getLogger(__name__)

            # If we are in a subprocess, use the handlers as setup by the multiprocessing library
            if multiprocessing.parent_process() is not None:
                self._logger.handlers = multiprocessing.get_logger().handlers

        return self._logger

    @property
    def input_queue(self) -> StoppableQueue[InputT]:
        return self._input_queue

    def get_input(self) -> InputT | None:
        start = time.monotonic()
        maybe_task = self.input_queue.get()
        if maybe_task is None:
            return None

        end = time.monotonic()
        diff = end - start
        if diff >= 1.0:
            self.logger.warning("Waited %fs to retrieve task for %s", diff, self.name)

        return maybe_task

    def run(self) -> None:
        # Reset logging for task
        self._logger = None

        while True:
            task_input = self.get_input()
            if task_input is None:
                logger.info("Worker signaled to stop... exiting!")
                return

            try:
                if not self.process(task_input):
                    self.logger.warning("Processing task signalled for worker shutdown... exiting!")
                    return
            except KeyboardInterrupt:
                self.logger.info("User signalled shutdown... exiting!")
                return
            except Exception:
                self.logger.exception("Failed to perform task...")

                # If we should stop work upon any exception... stop working!
                if self._exit_on_exception:
                    return


class BiWorker(Worker[InputT], Generic[InputT, OutputT]):
    def __init__(
        self,
        *,
        input_queue: StoppableQueue[InputT],
        output_queue: StoppableQueue[OutputT],
        logger: logging.Logger | None = None,
        exit_on_exception: bool = False,
    ):
        super().__init__(input_queue=input_queue, logger=logger, exit_on_exception=exit_on_exception)
        self._output_queue = output_queue

    @property
    def output_queue(self) -> StoppableQueue[OutputT]:
        return self._output_queue

    def put_output(self, output: OutputT) -> None:
        start = time.monotonic()
        self.output_queue.put(output)
        end = time.monotonic()
        diff = end - start
        if diff >= 1.0:
            self.logger.warning("Waited %fs to enqueue data for %s", diff, self.name)
