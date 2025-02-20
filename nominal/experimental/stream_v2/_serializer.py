from __future__ import annotations

from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from types import TracebackType
from typing import Type

from typing_extensions import Self

from nominal.core._batch_processor_proto import serialize_batch
from nominal.core._queueing import Batch
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class SerializedBatch:
    """Result of batch serialization containing the protobuf data and timestamp bounds."""

    data: bytes  # Serialized protobuf data
    oldest_timestamp: IntegralNanosecondsUTC  # Oldest timestamp in the batch
    newest_timestamp: IntegralNanosecondsUTC  # Newest timestamp in the batch


@dataclass(frozen=True)
class BatchSerializer:
    """Serialize batch write requests in separate processes.

    Protobuf creation and serialization can be CPU-intensive, so this allows spreading the load.
    """

    pool: ProcessPoolExecutor

    def close(self) -> None:
        self.pool.shutdown(cancel_futures=True)

    @classmethod
    def create(cls, max_workers: int) -> Self:
        pool = ProcessPoolExecutor(max_workers=max_workers)
        return cls(pool=pool)

    def serialize(self, batch: Batch) -> Future[SerializedBatch]:
        return self.pool.submit(serialize_batch, batch)

    def __enter__(self) -> BatchSerializer:
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()
