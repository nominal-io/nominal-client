from __future__ import annotations

import concurrent.futures
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from queue import Queue
from types import TracebackType
from typing import Protocol, Sequence, Type

from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService
from nominal.core._queueing import Batch, QueueShutdown, ReadQueue, iter_queue, spawn_batching_thread
from nominal.core.stream import BatchItem
from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.ts import IntegralNanosecondsUTC, _normalize_timestamp

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WriteStreamV2:
    _item_queue: Queue[BatchItem | QueueShutdown]
    _batch_thread: threading.Thread
    _write_pool: ThreadPoolExecutor
    _batch_serialize_thread: threading.Thread
    _serializer: BatchSerializer
    _clients: _Clients
    _track_metrics: bool

    class _Clients(HasAuthHeader, Protocol):
        @property
        def proto_write(self) -> ProtoWriteService: ...

    @classmethod
    def create(
        cls,
        clients: _Clients,
        serializer: BatchSerializer,
        nominal_data_source_rid: str,
        max_batch_size: int,
        max_wait: timedelta,
        max_queue_size: int,
        track_metrics: bool,
        max_workers: int | None,
    ) -> Self:
        write_pool = ThreadPoolExecutor(max_workers=max_workers)
        item_maxsize = max_queue_size if max_queue_size > 0 else 0
        batch_queue_maxsize = (max_queue_size // max_batch_size) if max_queue_size > 0 else 0

        item_queue: Queue[BatchItem | QueueShutdown] = Queue(maxsize=item_maxsize)
        batch_thread, batch_queue = spawn_batching_thread(
            item_queue,
            max_batch_size,
            max_wait,
            max_queue_size=batch_queue_maxsize,
        )
        batch_serialize_thread = spawn_batch_serialize_thread(
            write_pool, clients, serializer, nominal_data_source_rid, batch_queue, item_queue, track_metrics
        )
        return cls(
            _write_pool=write_pool,
            _item_queue=item_queue,
            _batch_thread=batch_thread,
            _batch_serialize_thread=batch_serialize_thread,
            _serializer=serializer,
            _track_metrics=track_metrics,
            _clients=clients,
        )

    def close(self) -> None:
        self._item_queue.put(QueueShutdown())
        self._batch_thread.join()
        self._batch_serialize_thread.join()
        self._serializer.close()
        self._write_pool.shutdown(cancel_futures=True)

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single value."""
        timestamp_normalized = _normalize_timestamp(timestamp)

        item = BatchItem(channel_name, timestamp_normalized, value, tags)
        self._item_queue.put(item)

    def enqueue_batch(
        self,
        channel_name: str,
        timestamps: Sequence[str | datetime | IntegralNanosecondsUTC],
        values: Sequence[float | str],
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write multiple values."""
        if len(timestamps) != len(values):
            raise ValueError(
                f"Expected equal numbers of timestamps and values! Received: {len(timestamps)} vs. {len(values)}"
            )

        for timestamp, value in zip(timestamps, values):
            self.enqueue(channel_name, timestamp, value, tags)

    def enqueue_from_dict(
        self,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        channel_values: dict[str, float | str],
    ) -> None:
        """Write multiple channel values at a single timestamp using a flattened dictionary.

        Each key in the dictionary is treated as a channel name and
        the corresponding value is enqueued with the provided timestamp.

        Args:
            timestamp: The common timestamp to use for all enqueued items.
            channel_values: A dictionary mapping channel names to their values.
        """
        timestamp_normalized = _normalize_timestamp(timestamp)
        enqueue_dict_timestamp_diff = timedelta(seconds=timestamp_normalized.timestamp() - datetime.now().timestamp())

        for channel, value in channel_values.items():
            self.enqueue(channel, timestamp, value)
        last_enqueue_timestamp = timedelta(seconds=timestamp_normalized.timestamp() - datetime.now().timestamp())

        if self._track_metrics:
            self._item_queue.put(
                BatchItem(
                    channel_name="enque_dict_start_staleness",
                    timestamp=timestamp_normalized,
                    value=enqueue_dict_timestamp_diff.total_seconds(),
                )
            )
            self._item_queue.put(
                BatchItem(
                    channel_name="enque_dict_end_staleness",
                    timestamp=timestamp_normalized,
                    value=last_enqueue_timestamp.total_seconds(),
                )
            )

    def __enter__(self) -> WriteStreamV2:
        """Create the stream as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def _write_serialized_batch(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    nominal_data_source_rid: str,
    item_queue: Queue[BatchItem | QueueShutdown],
    track_metrics: bool,
    future: concurrent.futures.Future[tuple[bytes, datetime, datetime]],
) -> None:
    try:
        serialized_data, most_recent_timestamp, least_recent_timestamp = future.result()
        write_future = pool.submit(
            clients.proto_write.write_nominal_batches_with_metrics,
            clients.auth_header,
            nominal_data_source_rid,
            serialized_data,
            most_recent_timestamp,
            least_recent_timestamp,
        )

        def on_write_complete(
            f: concurrent.futures.Future[tuple[timedelta, timedelta, timedelta, timedelta, timedelta]],
        ) -> None:
            try:
                (
                    least_recent_before_request_diff,
                    most_recent_before_request_diff,
                    rtt,
                    oldest_total_rtt,
                    newest_total_rtt,
                ) = f.result()  # Check for exceptions

                current_time = datetime.now()
                item_queue.put(
                    BatchItem(
                        channel_name="least_recent_before_request_diff",
                        timestamp=current_time,
                        value=least_recent_before_request_diff.total_seconds(),
                    )
                )
                item_queue.put(
                    BatchItem(
                        channel_name="most_recent_before_request_diff",
                        timestamp=current_time,
                        value=most_recent_before_request_diff.total_seconds(),
                    )
                )
                item_queue.put(
                    BatchItem(
                        channel_name="rtt",
                        timestamp=current_time,
                        value=rtt.total_seconds(),
                    )
                )
                item_queue.put(
                    BatchItem(
                        channel_name="oldest_total_rtt",
                        timestamp=current_time,
                        value=oldest_total_rtt.total_seconds(),
                    )
                )
                item_queue.put(
                    BatchItem(
                        channel_name="newest_total_rtt",
                        timestamp=current_time,
                        value=newest_total_rtt.total_seconds(),
                    )
                )
            except Exception as e:
                logger.error(f"Error in write completion callback: {e}", exc_info=True)

        if track_metrics:
            write_future.add_done_callback(on_write_complete)
    except Exception as e:
        logger.error(f"Error processing batch: {e}", exc_info=True)
        raise e


def serialize_and_write_batches(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    item_queue: Queue[BatchItem | QueueShutdown],
    batch_queue: ReadQueue[Batch[BatchItem]],
    track_metrics: bool,
) -> None:
    """Worker that processes batches."""
    callback = partial(_write_serialized_batch, pool, clients, nominal_data_source_rid, item_queue, track_metrics)
    for batch in iter_queue(batch_queue):
        future = serializer.serialize(batch)
        future.add_done_callback(callback)


def spawn_batch_serialize_thread(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    batch_queue: ReadQueue[Batch[BatchItem]],
    item_queue: Queue[BatchItem | QueueShutdown],
    track_metrics: bool,
) -> threading.Thread:
    thread = threading.Thread(
        target=serialize_and_write_batches,
        args=(pool, clients, serializer, nominal_data_source_rid, item_queue, batch_queue, track_metrics),
    )
    thread.start()
    return thread
