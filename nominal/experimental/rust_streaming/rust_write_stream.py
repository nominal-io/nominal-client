from __future__ import annotations

import datetime
import pathlib

from nominal_streaming import NominalDatasetStream

from nominal.core._stream.write_stream import DataStream
from nominal.core._types import PathLike
from nominal.core.datasource import DataSource


class RustWriteStream(NominalDatasetStream, DataStream):
    """Thin wrapper around the existing Rust Dataset Stream.

    See: `nominal_streaming.NominalDatasetStream` for more details
    """

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

        return stream
