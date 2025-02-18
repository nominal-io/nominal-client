from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from types import TracebackType
from typing import Type

from typing_extensions import Self

from nominal.core._batch_processor_proto import serialize_batch
from nominal.core.stream import BatchItem


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

    def serialize(self, batch: Sequence[BatchItem]) -> Future[bytes]:
        return self.pool.submit(serialize_batch, batch)

    def __enter__(self) -> BatchSerializer:
        return self

    def __exit__(
        self, exc_type: Type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> bool | None:
        self.close()
        if exc_value is not None:
            return False
