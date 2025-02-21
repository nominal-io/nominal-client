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
from typing import Callable, Protocol, Sequence, Type

from typing_extensions import Self

from nominal.core._batch_processor_proto import SerializedBatch
from nominal.core._clientsbunch import HasAuthHeader, ProtoWriteService, RequestMetrics
from nominal.core._queueing import Batch, QueueShutdown, ReadQueue, iter_queue, spawn_batching_thread
from nominal.core.stream import BatchItem
from nominal.experimental.stream_v2._serializer import BatchSerializer
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


@dataclass()
class MetricsManager:
    _item_queue: Queue[BatchItem | QueueShutdown]
    _enabled: bool
    _counter: int = 0

    def add_metric(self, channel_name: str, timestamp: IntegralNanosecondsUTC, value: float) -> None:
        if not self._enabled:
            return

        self._item_queue.put(
            BatchItem(
                channel_name=channel_name,
                timestamp=timestamp,
                value=value / 1e9,
            )
        )

    def track_enqueue_latency(self, timestamp_normalized: int) -> None:
        if not self._enabled:
            return

        self._counter += 1
        if self._counter >= 1000:
            current_time_ns = time.time_ns()
            staleness = (current_time_ns - timestamp_normalized) / 1e9
            self.add_metric("point_staleness", timestamp_normalized, staleness)
            self._counter = 0


@dataclass(frozen=True)
class WriteStreamV2:
    _item_queue: Queue[BatchItem | QueueShutdown]
    _batch_thread: threading.Thread
    _write_pool: ThreadPoolExecutor
    _batch_serialize_thread: threading.Thread
    _serializer: BatchSerializer
    _clients: _Clients
    _metrics_manager: MetricsManager

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
        metrics_manager = MetricsManager(
            _item_queue=item_queue,
            _enabled=track_metrics,
        )

        batch_thread, batch_queue = spawn_batching_thread(
            item_queue,
            max_batch_size,
            max_wait,
            max_queue_size=batch_queue_maxsize,
        )
        batch_serialize_thread = spawn_batch_serialize_thread(
            write_pool, clients, serializer, nominal_data_source_rid, batch_queue, metrics_manager
        )

        return cls(
            _write_pool=write_pool,
            _item_queue=item_queue,
            _batch_thread=batch_thread,
            _batch_serialize_thread=batch_serialize_thread,
            _serializer=serializer,
            _clients=clients,
            _metrics_manager=metrics_manager,
        )

    def close(self) -> None:
        self._item_queue.put(QueueShutdown())
        self._batch_thread.join()

        self._serializer.close()
        self._write_pool.shutdown(cancel_futures=True)

        self._batch_serialize_thread.join()

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime | IntegralNanosecondsUTC,
        value: float | str,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Write a single value."""
        timestamp_normalized = _SecondsNanos.from_flexible(timestamp).to_nanoseconds()

        self._metrics_manager.track_enqueue_latency(timestamp_normalized)

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
        """Write multiple channel values at a single timestamp using a flattened dictionary."""
        for channel, value in channel_values.items():
            self.enqueue(channel, timestamp, value)

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
    metrics_manager: MetricsManager,
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
    metrics_manager: MetricsManager,
    f: concurrent.futures.Future[RequestMetrics],
) -> None:
    try:
        metrics = f.result()
        current_time_ns = time.time_ns()
        metrics_manager.add_metric(
            "__nominal.metric.largest_latency_before_request", current_time_ns, metrics.largest_latency_before_request
        )
        metrics_manager.add_metric(
            "__nominal.metric.smallest_latency_before_request", current_time_ns, metrics.smallest_latency_before_request
        )
        metrics_manager.add_metric("__nominal.metric.request_rtt", current_time_ns, metrics.request_rtt)
        metrics_manager.add_metric(
            "__nominal.metric.largest_latency_after_request", current_time_ns, metrics.largest_latency_after_request
        )
        metrics_manager.add_metric(
            "__nominal.metric.smallest_latency_after_request", current_time_ns, metrics.smallest_latency_after_request
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
    metrics_manager: MetricsManager,
    batch_queue: ReadQueue[Batch],
) -> None:
    """Worker that processes batches."""
    write_callback = (
        partial(_on_write_complete_with_metrics, metrics_manager)
        if metrics_manager._enabled
        else _on_write_complete_noop
    )
    callback = partial(_write_serialized_batch, pool, clients, nominal_data_source_rid, metrics_manager, write_callback)
    for batch in iter_queue(batch_queue):
        future = serializer.serialize(batch)
        future.add_done_callback(callback)


def spawn_batch_serialize_thread(
    pool: ThreadPoolExecutor,
    clients: WriteStreamV2._Clients,
    serializer: BatchSerializer,
    nominal_data_source_rid: str,
    batch_queue: ReadQueue[Batch],
    metrics_manager: MetricsManager,
) -> threading.Thread:
    thread = threading.Thread(
        target=serialize_and_write_batches,
        args=(pool, clients, serializer, nominal_data_source_rid, metrics_manager, batch_queue),
    )
    thread.start()
    return thread
