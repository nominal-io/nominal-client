from __future__ import annotations

import logging
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from types import TracebackType
from typing import Type

from typing_extensions import Self

from nominal.core._batch_processor_proto import SerializedBatch, serialize_batch
from nominal.core._queueing import Batch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchSerializer:
    """Serialize batch write requests in separate processes.

    Protobuf creation and serialization can be CPU-intensive, so this allows spreading the load.
    """

    pool: ProcessPoolExecutor

    def close(self) -> None:
        self.pool.shutdown(cancel_futures=True)
        # processes = getattr(self.pool, "_processes", None)
        # if processes is not None:
        #     for pid, proc in processes.items():
        #         if proc.is_alive():
        #             logger.info("Force terminating process %s", pid)
        #             proc.terminate()
        #             proc.join()
        # else:
        #     logger.warning("ProcessPoolExecutor._processes is unavailable; unable to force terminate processes.")

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
