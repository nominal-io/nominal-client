from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from queue import Queue
from types import TracebackType
from typing import Callable, Mapping, Protocol, Type

from typing_extensions import Self

from nominal.core._batch_processor_proto import SerializedBatch
from nominal.core._clientsbunch import HasScoutParams, ProtoWriteService, RequestMetrics
from nominal.core._queueing import Batch, QueueShutdown, ReadQueue, iter_queue, spawn_batching_thread
from nominal.core.stream import BatchItem
from nominal.core.write_stream_base import WriteStreamBase
from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WriteStreamV2(WriteStreamBase):
    _item_queue: Queue[BatchItem | QueueShutdown]
    _batch_thread: threading.Thread
    _write_pool: ThreadPoolExecutor
    _batch_serialize_thread: threading.Thread
    _serializer: BatchSerializer
    _clients: _Clients
    _track_metrics: bool
    _add_metric: Callable[[str, int, float], None]

    class _Clients(HasScoutParams, Protocol):
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

        def add_metric_impl(channel_name: str, timestamp: IntegralNanosecondsUTC, value: float) -> None:
            item_queue.put(
                BatchItem(
                    channel_name=channel_name,
                    timestamp=timestamp,
                    value=value,
                )
            )

        def add_metric_noop(channel_name: str, timestamp: int, value: float) -> None:
            pass

        add_metric_fn = add_metric_impl if track_metrics else add_metric_noop

        return cls(
            _write_pool=write_pool,
            _item_queue=item_queue,
            _batch_thread=batch_thread,
            _batch_serialize_thread=batch_serialize_thread,
            _serializer=serializer,
            _track_metrics=track_metrics,
            _clients=clients,
            _add_metric=add_metric_fn,
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
        self.close(wait=exc_type is None)

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single value."""
        timestamp_normalized = _SecondsNanos.from_flexible(timestamp).to_nanoseconds()

        item = BatchItem(channel_name, timestamp_normalized, value, tags)
        self._item_queue.put(item)

    def enqueue_from_dict(
        self,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        channel_values: Mapping[str, float | str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write multiple channel values at a single timestamp using a flattened dictionary.

        Each key in the dictionary is treated as a channel name and
        the corresponding value is enqueued with the provided timestamp.

        Args:
            timestamp: The common timestamp to use for all enqueued items.
            channel_values: A dictionary mapping channel names to their values.
            tags: Key-value tags associated with the data being uploaded.
                NOTE: This *should* include all `required_tags` used when creating a `Connection` to Nominal.
        """
        timestamp_normalized = _SecondsNanos.from_flexible(timestamp).to_nanoseconds()
        current_time_ns = time.time_ns()
        enqueue_dict_timestamp_diff = current_time_ns - timestamp_normalized

        super().enqueue_from_dict(timestamp, channel_values, tags)

        current_time_ns = time.time_ns()
        last_enqueue_timestamp_diff = current_time_ns - timestamp_normalized

        self._add_metric_impl("enque_dict_start_staleness", timestamp_normalized, enqueue_dict_timestamp_diff / 1e9)
        self._add_metric_impl("enque_dict_end_staleness", timestamp_normalized, last_enqueue_timestamp_diff / 1e9)

    def close(self, wait: bool = True) -> None:
        logger.debug("Closing write stream (wait=%s)", wait)
        self._item_queue.put(QueueShutdown())
        self._batch_thread.join()

        self._serializer.close(cancel_futures=not wait)
        self._write_pool.shutdown(cancel_futures=not wait)

        self._batch_serialize_thread.join()

    def _add_metric_impl(self, channel_name: str, timestamp: IntegralNanosecondsUTC, value: float) -> None:
        """Add a metric using the configured implementation."""
        self._add_metric(channel_name, timestamp, value)


def _write_serialized_batch(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    nominal_data_source_rid: str,
    item_queue: Queue[BatchItem | QueueShutdown],
    write_callback: Callable[[concurrent.futures.Future[RequestMetrics]], None],
    future: concurrent.futures.Future[SerializedBatch],
) -> None:
    try:
        serialized = future.result()
        write_future = pool.submit(
            clients.proto_write.write_nominal_batches_with_metrics,
            clients.auth_header,
            nominal_data_source_rid,
            serialized.data,
            serialized.oldest_timestamp,
            serialized.newest_timestamp,
        )
        write_future.add_done_callback(write_callback)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt caught in _write_serialized_batch; aborting batch write.")
        return
    except Exception as e:
        logger.error(f"Error processing batch: {e}", exc_info=True)
        raise e


def _on_write_complete_with_metrics(
    item_queue: Queue[BatchItem | QueueShutdown],
    f: concurrent.futures.Future[RequestMetrics],
) -> None:
    try:
        metrics = f.result()
        current_time_ns = time.time_ns()
        item_queue.put(
            BatchItem(
                channel_name="__nominal.metric.largest_latency_before_request",
                timestamp=current_time_ns,
                value=metrics.largest_latency_before_request,
            )
        )
        item_queue.put(
            BatchItem(
                channel_name="__nominal.metric.smallest_latency_before_request",
                timestamp=current_time_ns,
                value=metrics.smallest_latency_before_request,
            )
        )
        item_queue.put(
            BatchItem(
                channel_name="__nominal.metric.request_rtt",
                timestamp=current_time_ns,
                value=metrics.request_rtt,
            )
        )
        item_queue.put(
            BatchItem(
                channel_name="__nominal.metric.largest_latency_after_request",
                timestamp=current_time_ns,
                value=metrics.largest_latency_after_request,
            )
        )
        item_queue.put(
            BatchItem(
                channel_name="__nominal.metric.smallest_latency_after_request",
                timestamp=current_time_ns,
                value=metrics.smallest_latency_after_request,
            )
        )
    except Exception as e:
        logger.error(f"Error in write completion callback: {e}", exc_info=True)


def _on_write_complete_noop(
    _: concurrent.futures.Future[RequestMetrics],
) -> None:
    pass


def serialize_and_write_batches(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    item_queue: Queue[BatchItem | QueueShutdown],
    batch_queue: ReadQueue[Batch],
    track_metrics: bool,
) -> None:
    """Worker that processes batches."""
    write_callback = partial(_on_write_complete_with_metrics, item_queue) if track_metrics else _on_write_complete_noop
    callback = partial(_write_serialized_batch, pool, clients, nominal_data_source_rid, item_queue, write_callback)
    for batch in iter_queue(batch_queue):
        future = serializer.serialize(batch)
        future.add_done_callback(callback)


def spawn_batch_serialize_thread(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    batch_queue: ReadQueue[Batch],
    item_queue: Queue[BatchItem | QueueShutdown],
    track_metrics: bool,
) -> threading.Thread:
    thread = threading.Thread(
        target=serialize_and_write_batches,
        args=(pool, clients, serializer, nominal_data_source_rid, item_queue, batch_queue, track_metrics),
    )
    thread.start()
    return thread
