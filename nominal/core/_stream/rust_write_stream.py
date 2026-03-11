from __future__ import annotations

import datetime
import pathlib
from typing import TYPE_CHECKING, Any, Mapping

from nominal.core._stream.write_stream import DataStream
from nominal.core._types import PathLike
from nominal.ts import IntegralNanosecondsUTC

if TYPE_CHECKING:
    from nominal.core.datasource import DataSource


class RustWriteStream(DataStream):
    """High-performance write stream backed by the nominal_streaming Rust library.

    This is the recommended write stream implementation. It delegates batching,
    serialization, and transport to the Rust ``nominal_streaming`` crate — the same
    backend used by Nominal Connect — for maximum throughput and reliability.

    Scalar value streaming (str, float, int) is fully supported.
    Array and struct streaming are not yet supported by the Rust backend;
    use ``data_format='protobuf'`` for those use cases.
    """

    _inner: Any  # NominalDatasetStream — typed as Any to allow lazy import

    def __init__(self, inner: Any) -> None:
        self._inner = inner

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
        from nominal_streaming import NominalDatasetStream

        kwargs = {}
        if num_workers:
            kwargs["num_upload_workers"] = num_workers
            kwargs["num_runtime_workers"] = num_workers

        api_key = datasource_clients.auth_header.removeprefix("Bearer ")
        stream = NominalDatasetStream.create(
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

        return cls(stream)

    def __enter__(self) -> RustWriteStream:
        self._inner.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        self._inner.__exit__(exc_type, exc_value, traceback)

    def enqueue(
        self,
        channel_name: str,
        timestamp: str | datetime.datetime | IntegralNanosecondsUTC,
        value: str | float | int,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single scalar value to the stream."""
        self._inner.enqueue(channel_name, timestamp, value, tags)

    def enqueue_float_array(
        self,
        channel_name: str,
        timestamp: str | datetime.datetime | IntegralNanosecondsUTC,
        value: list[float],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Not yet supported by the Rust streaming backend.

        Raises:
            NotImplementedError: Always. Use data_format='protobuf' for array streaming.
        """
        raise NotImplementedError(
            "Array streaming is not yet supported by the Rust streaming backend. "
            "Use data_format='protobuf' for array streaming."
        )

    def enqueue_string_array(
        self,
        channel_name: str,
        timestamp: str | datetime.datetime | IntegralNanosecondsUTC,
        value: list[str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Not yet supported by the Rust streaming backend.

        Raises:
            NotImplementedError: Always. Use data_format='protobuf' for array streaming.
        """
        raise NotImplementedError(
            "Array streaming is not yet supported by the Rust streaming backend. "
            "Use data_format='protobuf' for array streaming."
        )

    def enqueue_struct(
        self,
        channel_name: str,
        timestamp: str | datetime.datetime | IntegralNanosecondsUTC,
        value: dict[str, Any],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Not yet supported by the Rust streaming backend.

        Raises:
            NotImplementedError: Always. Use data_format='protobuf' for struct streaming.
        """
        raise NotImplementedError(
            "Struct streaming is not yet supported by the Rust streaming backend. "
            "Use data_format='protobuf' for struct streaming."
        )

    def close(self, wait: bool = True) -> None:
        """Close the stream, flushing any buffered data."""
        self._inner.close(wait=wait)
