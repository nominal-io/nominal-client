from __future__ import annotations

import logging
import multiprocessing
from dataclasses import dataclass
from typing import Sequence, TypeVar

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
