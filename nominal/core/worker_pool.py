from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import Executor, Future
from dataclasses import dataclass
from typing import Any, Callable, Sequence, TypeVar

from nominal.core._clientsbunch import ProtoWriteService
from nominal.core.stream import BatchItem

logger = logging.getLogger(__name__)

# Add global variable declaration at the top level
WORKER_CONTEXT: WorkerContext | None = None


_T = TypeVar("_T")


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
        f"%(asctime)s - Process-{multiprocessing.current_process().name} - %(levelname)s - %(message)s"
    )
    for handler in logger.handlers:
        handler.setFormatter(formatter)


def process_batch_worker(batch: Sequence[BatchItem]) -> None:
    from nominal.core.batch_processor_proto import process_batch

    if WORKER_CONTEXT is None:
        raise RuntimeError("Worker context not initialized")

    process_batch(
        batch=batch,
        nominal_data_source_rid=WORKER_CONTEXT.nominal_data_source_rid,
        auth_header=WORKER_CONTEXT.auth_header,
        proto_write=WORKER_CONTEXT.proto_write,
    )


class ProcessPoolManager(Executor):
    def __init__(
        self,
        max_workers: int,
        client_factory: Callable[[], ProtoWriteService],
        nominal_data_source_rid: str,
        auth_header: str,
    ):
        """Initialize the process pool manager.

        Args:
            max_workers (int): The maximum number of worker threads to create.
            client_factory (Callable[[], ProtoWriteService]): A factory function that creates ProtoWriteService.
            nominal_data_source_rid (str): The nominal data source rid.
            auth_header (str): The authentication header.
        """
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
            initargs=(self.contexts[0],),
        )

    def submit(self, fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> Future[_T]:
        """Submit a task to the process pool and return a Future."""
        future: Future[_T] = Future()

        def _callback(result: Any) -> None:
            future.set_result(result)

        def _error_callback(error: BaseException) -> None:
            future.set_exception(error)

        async_result = self.pool.apply_async(fn, args=args, callback=_callback, error_callback=_error_callback)

        # Store async_result as an attribute of the future using __dict__ to avoid type errors
        future.__dict__["async_result"] = async_result

        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Shutdown the process pool."""
        if wait:
            self.pool.close()
            self.pool.join()
        else:
            self.pool.terminate()
