from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core.stream import BatchItem

logger = logging.getLogger(__name__)

@dataclass
class WorkerContext:
    nominal_data_source_rid: str
    auth_header: str
    proto_write: ProtoWriteService

def worker_init(context: WorkerContext) -> None:
    global WORKER_CONTEXT
    WORKER_CONTEXT = context
    
    # Configure process-specific logging
    logger = logging.getLogger()
    formatter = logging.Formatter(
        f'%(asctime)s - Process-{multiprocessing.current_process().name} - %(levelname)s - %(message)s'
    )
    for handler in logger.handlers:
        handler.setFormatter(formatter)

def process_batch_worker(batch: Sequence[BatchItem]) -> None:
    from nominal.core.batch_processor_proto import process_batch
    global WORKER_CONTEXT
    
    process_batch(
        batch=batch,
        nominal_data_source_rid=WORKER_CONTEXT.nominal_data_source_rid,
        auth_header=WORKER_CONTEXT.auth_header,
        proto_write=WORKER_CONTEXT.proto_write,
    )

class ProcessPoolManager:
    def __init__(
        self,
        max_workers: int,
        client_factory: Callable[[], ProtoWriteService],
        nominal_data_source_rid: str,
        auth_header: str,
    ):
        self.max_workers = max_workers
        self.contexts = [
            WorkerContext(
                nominal_data_source_rid=nominal_data_source_rid,
                auth_header=auth_header,
                proto_write=client_factory(),
            )
            for _ in range(max_workers)
        ]
        
        self.pool = multiprocessing.Pool(
            processes=max_workers,
            initializer=worker_init,
            initargs=(self.contexts[0],),  # Each process gets same context for now
        )

    def submit(self, fn: Callable, *args: Any, **kwargs: Any) -> Future:
        """Submit a task to the process pool and return a Future."""
        future = Future()
        
        def _callback(result: Any) -> None:
            future.set_result(result)
            
        def _error_callback(error: BaseException) -> None:
            future.set_exception(error)
            
        async_result = self.pool.apply_async(
            fn, 
            args=args, 
            callback=_callback,
            error_callback=_error_callback
        )
        
        # Store the AsyncResult to prevent it from being garbage collected
        future.async_result = async_result  # type: ignore
        
        return future
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the process pool."""
        if wait:
            self.pool.close()
            self.pool.join()
        else:
            self.pool.terminate() 
