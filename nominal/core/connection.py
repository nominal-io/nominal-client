from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, Sequence, overload

from nominal_api import scout_datasource_connection_api

from nominal.core._stream.write_stream import DataStream
from nominal.core._types import PathLike
from nominal.core.datasource import DataSource, _get_write_stream


@dataclass(frozen=True)
class Connection(DataSource):
    name: str
    description: str | None

    @classmethod
    def _from_conjure(
        cls, clients: DataSource._Clients, response: scout_datasource_connection_api.Connection
    ) -> Connection | StreamingConnection:
        """Factory method to create the appropriate Connection subclass based on connection details"""
        if response.connection_details.nominal is not None:
            return StreamingConnection(
                rid=response.rid,
                name=response.display_name,
                description=response.description,
                _clients=clients,
                nominal_data_source_rid=response.connection_details.nominal.nominal_data_source_rid,
            )
        return cls(
            rid=response.rid,
            name=response.display_name,
            description=response.description,
            _clients=clients,
        )

    def archive(self) -> None:
        """Archive this connection.
        Archived connections are not deleted, but are hidden from the UI.
        """
        self._clients.connection.archive_connection(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this connection, making it visible in the UI."""
        self._clients.connection.unarchive_connection(self._clients.auth_header, self.rid)


@dataclass(frozen=True)
class StreamingConnection(Connection):
    """A `StreamingConnection` is used to stream telemetry data to Nominal.

    This method of streaming is being phased out in favor of streaming to a dataset.
    However, it is still available while we complete adding the same level of tag
    support to datasets, and for backwards compatibility.
    """

    nominal_data_source_rid: str

    @overload
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        implementation: Literal["json", "protobuf", "experimental"] = ...,
        *,
        data_format: Literal["json", "protobuf", "experimental"] | None = None,
    ) -> DataStream: ...
    @overload
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        implementation: Literal["rust", "rust_experimental"] | None = None,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
        *,
        data_format: Literal["rust", "rust_experimental"] | None = None,
    ) -> DataStream: ...
    def get_write_stream(
        self,
        batch_size: int = 250_000,
        max_wait: timedelta = timedelta(seconds=0.25),
        implementation: Literal["json", "protobuf", "experimental", "rust", "rust_experimental"] | None = None,
        file_fallback: PathLike | None = None,
        log_level: str | None = None,
        num_workers: int | None = None,
        *,
        data_format: Literal["json", "protobuf", "experimental", "rust", "rust_experimental"] | None = None,
    ) -> DataStream:
        """Stream to write non-blocking messages to a datasource.

        Args:
        ----
            batch_size: How big the batch can get before writing to Nominal.
            max_wait: How long a batch can exist before being flushed to Nominal.
            implementation: Streaming implementation to use. Defaults to 'rust', falling back to
                'protobuf' when `nominal-streaming` is not installed.
                NOTE: 'protobuf', 'experimental', and 'rust_experimental' are deprecated; 'rust'
                      supersedes all three.
            file_fallback: Filepath to write failed batches to during streaming
                NOTE: expects a .avro filename
                NOTE: only works with `implementation='rust'`
            log_level: Log level to use in underlying rust streaming code.
                NOTE: Should be a rust log level e.g. 'debug', 'trace', 'info', etc.
                NOTE: only works with `implementation='rust'`
            num_workers: Number of worker threads to use in underlying rust streaming code.
                NOTE: use with care-- this may have large impacts on streaming performance.
                NOTE: only works with `implementation='rust'`
            data_format: Deprecated name for `implementation`. Passing both is an error.

        Returns:
        --------
            Write stream object configured to send data to nominal. This may be used as a context manager
            (so that resources are automatically released upon exiting the context), or if not used as a context
            manager, should be explicitly `close()`-ed once no longer needed.

        Raises:
        ------
            ValueError: both `implementation` and `data_format` were given, or the implementation is unknown.
            ImportError: `nominal-streaming` is not installed and rust streaming was asked for, either
                explicitly via `implementation` or implicitly by passing a rust-only argument.
        """
        return _get_write_stream(
            batch_size=batch_size,
            max_wait=max_wait,
            implementation=implementation,
            data_format=data_format,
            file_fallback=file_fallback,
            log_level=log_level,
            num_workers=num_workers,
            write_rid=self.nominal_data_source_rid,
            clients=self._clients,
        )


def _get_connections(
    clients: Connection._Clients, connection_rids: Sequence[str]
) -> Sequence[scout_datasource_connection_api.Connection]:
    return [clients.connection.get_connection(clients.auth_header, rid) for rid in connection_rids]


def _get_connection(clients: Connection._Clients, connection_rid: str) -> scout_datasource_connection_api.Connection:
    return clients.connection.get_connection(clients.auth_header, connection_rid)
