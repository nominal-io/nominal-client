from __future__ import annotations

import logging
import concurrent.futures
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass

from typing_extensions import Self

from nominal.core._batch_processor_proto import SerializedBatch, SerializedBatchV2, serialize_batch, serialize_batch_v2
from nominal.core._queueing import Batch, BatchV2

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class BatchSerializer:
    """Serialize batch write requests in separate processes.

    Protobuf creation and serialization can be CPU-intensive, so this allows spreading the load.
    This implementation includes error handling and process management to avoid BrokenProcessPool errors.
    """

    pool: ProcessPoolExecutor

    def close(self, cancel_futures: bool = False) -> None:
        self.pool.shutdown(wait=True, cancel_futures=cancel_futures)

    @classmethod
    def create(cls, max_workers: int) -> Self:
        """Create a new BatchSerializer.
        
        Args:
            max_workers: Number of worker processes to use for serialization
            maxtasksperchild: Maximum number of tasks a worker process can complete before 
                being replaced to prevent memory leaks (None means unlimited)
        """
        # Create a process pool with automatic process replacement
        pool = ProcessPoolExecutor(
            max_workers=max_workers, 

        )
        return cls(pool=pool)

    def serialize(self, batch: Batch) -> Future[SerializedBatch]:
        """Serialize a batch in a separate process with error handling.
        
        Returns a future that will contain the serialized batch or raise an exception
        if serialization fails.
        """
        return self.pool.submit(serialize_batch, batch)

    def serialize_v2(self, batch: BatchV2) -> concurrent.futures.Future[SerializedBatchV2]:
        try:
            return self.pool.submit(serialize_batch_v2, batch)
        except Exception as e:
            logger.error(f"Serialization error: {e}", exc_info=True)
            raise

    


    def __enter__(self) -> BatchSerializer:
        return self
