from __future__ import annotations

import datetime
import importlib.metadata
import pathlib
import re
import warnings
from typing import Any

from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._stream.write_stream import DataStream
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import USER_AGENT_NAME


def _nominal_release() -> str | None:
    """The installed `nominal` version reduced to its numeric release, e.g. `1.2.0` for `1.2.0.dev3`.

    The stream's user agent accepts only dot-separated numbers (optionally `-rcN`), so PEP 440
    pre-release, dev, and local segments are dropped. None when the version cannot be read.
    """
    try:
        version = importlib.metadata.version("nominal")
    except importlib.metadata.PackageNotFoundError:
        return None
    match = re.match(r"\d+(\.\d+)*", version)
    return match.group() if match else None


class RustWriteStream(NominalDatasetStream, DataStream):
    """Thin wrapper around the existing Rust Dataset Stream.

    See: `nominal_streaming.NominalDatasetStream` for more details
    """

    def __enter__(self) -> RustWriteStream:
        """Enter the stream, opening it only if it is not open already.

        `_from_dataset` opens the stream so that it accepts enqueues without a `with` block, the
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
    def _from_dataset(
        cls,
        dataset_rid: str,
        clients: HasScoutParams,
        batch_size: int,
        max_wait: datetime.timedelta,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
        track_metrics: bool = False,
    ) -> RustWriteStream:
        kwargs: dict[str, Any] = {}
        if num_workers:
            kwargs["num_upload_workers"] = num_workers
            kwargs["num_runtime_workers"] = num_workers

        opts = PyNominalStreamOpts(
            max_points_per_batch=batch_size,
            max_request_delay_secs=max_wait.total_seconds(),
            base_api_url=clients.api_base_url,
            track_metrics=track_metrics,
            **kwargs,
        )
        release = _nominal_release()
        if release is not None:
            opts = opts.with_user_agent(USER_AGENT_NAME, release)

        api_key = clients.auth_header.removeprefix("Bearer ")
        stream = cls(api_key, opts).with_core_consumer(dataset_rid)

        if file_fallback is not None:
            stream = stream.with_file_fallback(pathlib.Path(file_fallback))

        if log_level is not None:
            stream = stream.enable_logging(log_level)

        # Open here rather than in `__enter__` alone: `get_write_stream` documents that the stream
        # may be closed explicitly instead of being used as a context manager, and an unopened rust
        # stream rejects every enqueue.
        return stream.open()
