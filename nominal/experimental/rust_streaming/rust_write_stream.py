from __future__ import annotations

import datetime
import pathlib
import warnings

from nominal_streaming import NominalDatasetStream

from nominal.core._stream.write_stream import DataStream
from nominal.core._types import PathLike
from nominal.core.datasource import DataSource


class RustWriteStream(NominalDatasetStream, DataStream):
    """Thin wrapper around the existing Rust Dataset Stream.

    See: `nominal_streaming.NominalDatasetStream` for more details
    """

    def __enter__(self) -> RustWriteStream:
        """Enter the stream, opening it only if it is not open already.

        `_from_datasource` opens the stream so that it accepts enqueues without a `with` block, the
        way `WriteStream` does. The underlying `open()` refuses to run twice, so entering an
        already-open stream has to be a no-op rather than a second open.
        """
        if self._opened:
            return self

        self.open()
        return self

    def flush(self, wait: bool = False, timeout: float | None = None) -> None:
        """No-op: the rust stream manages its own flushing.

        `WriteStream.flush` is what `get_write_stream` used to return, so callers who never named an
        implementation may be calling this. It warns rather than raising: it is not on
        `WriteStreamBase`, so the callers who reach it are the untyped ones, and breaking them
        outright would skip the deprecation cycle they are owed.

        Batches are sent once they reach `batch_size` or `max_wait`, and `close()` drains what is
        left, so what `flush(wait=True)` guaranteed is still available through `close()`.
        """
        warnings.warn(
            "flush() does nothing on the rust implementation: batches are sent once they reach "
            "batch_size or max_wait, and close() drains what is left.",
            UserWarning,
            stacklevel=2,
        )

    @classmethod
    def _from_datasource(
        cls,
        datasource_rid: str,
        datasource_clients: DataSource._Clients,
        batch_size: int,
        max_wait: datetime.timedelta,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
    ) -> RustWriteStream:
        kwargs = {}
        if num_workers:
            kwargs["num_upload_workers"] = num_workers
            kwargs["num_runtime_workers"] = num_workers

        api_key = datasource_clients.auth_header.removeprefix("Bearer ")
        stream = cls.create(
            api_key,
            datasource_clients.storage_writer._uri,
            max_points_per_batch=batch_size,
            max_request_delay_secs=max_wait.total_seconds(),
            **kwargs,
        ).with_core_consumer(datasource_rid)

        if file_fallback is not None:
            stream = stream.with_file_fallback(pathlib.Path(file_fallback))

        if log_level is not None:
            stream = stream.enable_logging(log_level)

        # Open here rather than in `__enter__` alone: `get_write_stream` documents that the stream
        # may be closed explicitly instead of being used as a context manager, and an unopened rust
        # stream rejects every enqueue.
        return stream.open()
