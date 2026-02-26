from __future__ import annotations

from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass

from typing_extensions import Self

from nominal.core._stream.batch_processor_proto import SerializedBatch, serialize_batch
from nominal.core._stream.write_stream import StreamValueType
from nominal.core._utils.queueing import Batch


@dataclass(frozen=True)
class BatchSerializer:
    """Serialize batch write requests in separate processes.

    Protobuf creation and serialization can be CPU-intensive, so this allows spreading the load.
    """

    pool: ProcessPoolExecutor
    data_source_rid: str = ""

    def close(self, cancel_futures: bool = False) -> None:
        self.pool.shutdown(cancel_futures=cancel_futures)

    @classmethod
    def create(cls, max_workers: int | None, data_source_rid: str = "") -> Self:
        pool = ProcessPoolExecutor(max_workers=max_workers)
        return cls(pool=pool, data_source_rid=data_source_rid)

    def serialize(self, batch: Batch[StreamValueType]) -> Future[SerializedBatch]:
        return self.pool.submit(serialize_batch, batch, self.data_source_rid)

    def __enter__(self) -> BatchSerializer:
        return self
