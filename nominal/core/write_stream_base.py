from __future__ import annotations

import abc
from datetime import datetime
from types import TracebackType
from typing import Mapping, Sequence, Type

from typing_extensions import Self

from nominal.ts import IntegralNanosecondsUTC


class WriteStreamBase(abc.ABC):
    @abc.abstractmethod
    def __enter__(self) -> Self:
        """Create the stream as a context manager."""

    @abc.abstractmethod
    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the stream and close out any used system resources."""

    @abc.abstractmethod
    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single value to the stream

        Args:
            channel_name: Name of the channel to upload data for.
            timestamp: Absolute timestamp of the data being uploaded.
            value: Value to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
                NOTE: This *must* include all `required_tags` used when creating a `Connection` to Nominal.
        """

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[str | datetime | IntegralNanosecondsUTC],
        values: Sequence[float | str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add a sequence of messages to the queue to upload to Nominal.

        Messages are added one-by-one (with timestamp normalization) and flushed
        based on the batch conditions.

        Args:
            channel_name: Name of the channel to upload data for.
            timestamps: Absolute timestamps of the data being uploaded.
            values: Values to write to the specified channel.
            tags: Key-value tags associated with the data being uploaded.
                NOTE: This *must* include all `required_tags` used when creating a `Connection` to Nominal.
        """
        if len(timestamps) != len(values):
            raise ValueError(
                f"Expected equal numbers of timestamps and values! "
                f"Received {len(timestamps)} timestamp(s) vs. {len(values)} value(s)."
            )

        for timestamp, value in zip(timestamps, values):
            self.enqueue(channel_name, timestamp, value, tags)

    def enqueue_from_dict(
        self,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        channel_values: Mapping[str, float | str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write multiple channel values at a given timestamp using a flattened dictionary.

        Each key in the dictionary is treated as a channel name and the corresponding value
        is enqueued with the given timestamp.

        Args:
            timestamp: The shared timestamp to use for all items to enqueue.
            channel_values: A dictionary mapping channel names to their respective values.
            tags: Key-value tags associated with the data being uploaded.
                NOTE: This *should* include all `required_tags` used when creating a `Connection` to Nominal.
        """
        for channel, value in channel_values.items():
            self.enqueue(channel, timestamp, value, tags)

    @abc.abstractmethod
    def close(self, wait: bool = True) -> None:
        """Close the stream.
        Stops any process timeout threads, and flushes any remaining batches.

        Args:
            wait: If true, block until stream is closed.
        """
