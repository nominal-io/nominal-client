from __future__ import annotations

import dataclasses
import logging
import threading
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from itertools import islice
from typing import TYPE_CHECKING, Callable, Iterable, Iterator

from nominal.core.client import NominalClient
from nominal.core.datasource import DataSource

if TYPE_CHECKING:
    from nominal.thirdparty.polars.polars_export_handler import _ExportJob

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SignedExport:
    """A signed export job ready for download."""

    job: _ExportJob
    url: str
    file_size_bytes: int


class ExportPresigner:
    """Signs export jobs in parallel while preserving input order.

    Uses a sliding window of futures backed by a ThreadPoolExecutor.
    Up to *max_ahead* jobs are in-flight at once.  Results are yielded
    strictly in input order -- if job 3 finishes before job 2, the
    caller blocks until job 2 completes.

    Backpressure is automatic: when the consumer stops pulling from
    :meth:`sign_all`, the window fills and no new jobs are submitted.
    """

    def __init__(self, sign_fn: Callable[[_ExportJob], SignedExport], max_ahead: int = 8):
        """Initialize with a signing function and concurrency window size."""
        self._sign_fn = sign_fn
        self._max_ahead = max_ahead

    def sign_all(self, jobs: Iterable[_ExportJob]) -> Iterator[SignedExport]:
        """Yield SignedExports in input order, signing up to max_ahead concurrently.

        Signs jobs in parallel using a sliding window of futures. Results are
        yielded strictly in input order — if job 3 finishes before job 2, the
        caller blocks until job 2 completes.

        Args:
            jobs: Export jobs to sign. Consumed lazily.

        Yields:
            SignedExport for each job, in input order.

        Raises:
            Exception: If sign_fn raises for any job, pending futures in the
                window are cancelled and the exception propagates immediately.
                Already-running signings complete in the background.
        """
        signed_count = 0
        with ThreadPoolExecutor(max_workers=self._max_ahead) as pool:
            window: deque[Future[SignedExport]] = deque()
            job_iter = iter(jobs)

            try:
                # Fill initial window
                for job in islice(job_iter, self._max_ahead):
                    window.append(pool.submit(self._sign_fn, job))
                logger.debug("Presigner window filled with %d initial jobs", len(window))

                # Drain front, refill back
                while window:
                    result = window.popleft().result()
                    signed_count += 1
                    logger.debug("Signed job %d: %d bytes", signed_count, result.file_size_bytes)
                    yield result
                    next_job = next(job_iter, None)
                    if next_job is not None:
                        window.append(pool.submit(self._sign_fn, next_job))
            except (Exception, KeyboardInterrupt):
                # Cancel pending futures so pool.shutdown() doesn't block
                for fut in window:
                    fut.cancel()
                logger.debug(
                    "Presigner interrupted after %d signed jobs, cancelled %d pending",
                    signed_count, len(window),
                )
                raise


def create_export_signer(client: NominalClient) -> Callable[[_ExportJob], SignedExport]:
    """Create a signer that calls the Nominal presigned export API.

    The returned callable signs a single export job by calling
    generate_export_channel_data_presigned_link. DataSource lookups
    are cached per datasource RID with thread-safe double-check locking.

    Args:
        client: Nominal client for API access.

    Returns:
        A callable that takes an _ExportJob and returns a SignedExport.
    """
    ds_cache: dict[str, DataSource] = {}
    ds_lock = threading.Lock()

    def sign(job: _ExportJob) -> SignedExport:
        logger.debug("Signing export for datasource=%s, %d channels", job.datasource_rid, len(job.channel_names))
        # Fast path: check cache without lock
        datasource = ds_cache.get(job.datasource_rid)
        if datasource is None:
            # Slow path: fetch outside lock, then write inside lock
            ds = client.get_datasource(job.datasource_rid)
            with ds_lock:
                # Another thread may have populated it first — use theirs
                datasource = ds_cache.setdefault(job.datasource_rid, ds)

        request = job.export_request(datasource)
        response = client._clients.dataexport.generate_export_channel_data_presigned_link(
            client._clients.auth_header,
            request,
        )
        return SignedExport(
            job=job,
            url=response.presigned_url.url,
            file_size_bytes=response.file_size_bytes,
        )

    return sign
